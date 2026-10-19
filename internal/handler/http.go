// Package handler is the HTTP delivery layer (Fiber).
package handler

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/requestid"
	"github.com/inblack/order-tracking/internal/apperror"
	"github.com/inblack/order-tracking/internal/usecase"
)

type Handler struct {
	svc *usecase.OrderService
	log *slog.Logger
}

func NewApp(svc *usecase.OrderService, log *slog.Logger, devMode bool) *fiber.App {
	h := &Handler{svc: svc, log: log}
	app := fiber.New(fiber.Config{
		ErrorHandler: h.errorHandler,
		Immutable:    true,
		BodyLimit:    64 * 1024,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	})
	app.Use(recover.New())
	app.Use(requestid.New())
	app.Use(h.accessLog)

	app.Get("/healthz", func(c *fiber.Ctx) error { return c.JSON(fiber.Map{"status": "ok"}) })

	// Customer API
	app.Post("/orders", h.createOrder)
	app.Get("/orders", h.listOrders)
	app.Get("/orders/:orderNo", h.getOrder)
	app.Get("/orders/:orderNo/timeline", h.timeline)
	app.Post("/orders/:orderNo/report-issue", h.reportIssue)
	app.Patch("/orders/:orderNo/resolve", h.resolve)

	// Bank callback
	app.Post("/webhooks/payment", h.paymentWebhook)

	// Bot API (for external bots / manual scenario testing)
	app.Post("/bot/claim", h.botClaim)
	app.Post("/bot/orders/:orderNo/heartbeat", h.botHeartbeat)
	app.Post("/bot/orders/:orderNo/complete", h.botComplete)
	app.Post("/bot/orders/:orderNo/fail", h.botFail)

	// Ops: trigger maintenance tick immediately (demo / testing)
	app.Post("/admin/maintenance/run", h.runMaintenance)

	if devMode {
		// DEV ONLY: returns {body, signature} so manual tests can sign webhooks.
		app.Post("/dev/webhooks/sign", func(c *fiber.Ctx) error {
			body := string(c.Body())
			return c.JSON(fiber.Map{"body": body, "signature": usecase.Sign(svc.Config().WebhookSecret, []byte(body))})
		})
	}
	return app
}

func (h *Handler) ctx(c *fiber.Ctx) context.Context { return c.UserContext() }

func (h *Handler) accessLog(c *fiber.Ctx) error {
	start := time.Now()
	err := c.Next()
	status := c.Response().StatusCode()
	if err != nil {
		var ae *apperror.Error
		var fe *fiber.Error
		switch {
		case errors.As(err, &ae):
			status = ae.Status
		case errors.As(err, &fe):
			status = fe.Code
		default:
			status = 500
		}
	}
	attrs := []any{"requestId", c.Locals("requestid"), "method", c.Method(), "path", c.Path(),
		"status", status, "latencyMs", time.Since(start).Milliseconds()}
	if no := c.Params("orderNo"); no != "" {
		attrs = append(attrs, "orderNo", no)
	}
	h.log.Info("http request", attrs...)
	return err
}

func (h *Handler) errorHandler(c *fiber.Ctx, err error) error {
	var ae *apperror.Error
	if errors.As(err, &ae) {
		return c.Status(ae.Status).JSON(ae)
	}
	var fe *fiber.Error
	if errors.As(err, &fe) {
		code := "HTTP_ERROR"
		switch fe.Code {
		case 404:
			code = "ROUTE_NOT_FOUND"
		case 405:
			code = "METHOD_NOT_ALLOWED"
		case 413:
			code = "PAYLOAD_TOO_LARGE"
		}
		return c.Status(fe.Code).JSON(apperror.New(fe.Code, code, fe.Message, nil))
	}
	h.log.Error("unhandled error", "err", err, "path", c.Path(), "orderNo", c.Params("orderNo"))
	return c.Status(500).JSON(apperror.Internal())
}

func parseBody(c *fiber.Ctx, v any) error {
	if err := c.BodyParser(v); err != nil {
		return apperror.Validation(map[string]string{"body": "invalid JSON body"})
	}
	return nil
}

// ---- customer

func (h *Handler) createOrder(c *fiber.Ctx) error {
	var in usecase.CreateOrderInput
	if err := parseBody(c, &in); err != nil {
		return err
	}
	o, replayed, err := h.svc.CreateOrder(h.ctx(c), c.Get("Idempotency-Key"), in)
	if err != nil {
		return err
	}
	if replayed {
		c.Set("Idempotent-Replayed", "true")
		return c.Status(fiber.StatusOK).JSON(o)
	}
	c.Set("Location", "/orders/"+o.OrderNo)
	return c.Status(fiber.StatusCreated).JSON(o)
}

func (h *Handler) getOrder(c *fiber.Ctx) error {
	o, err := h.svc.GetOrder(h.ctx(c), c.Params("orderNo"))
	if err != nil {
		return err
	}
	return c.JSON(o)
}

func (h *Handler) listOrders(c *fiber.Ctx) error {
	page, perr := strconv.Atoi(c.Query("page", "1"))
	limit, lerr := strconv.Atoi(c.Query("limit", "10"))
	if perr != nil || lerr != nil {
		return apperror.Validation(map[string]string{"page/limit": "must be integers"})
	}
	userID := c.Query("user_id", c.Get("X-User-ID"))
	p, err := h.svc.ListOrders(h.ctx(c), c.Query("tab", "current"), userID, page, limit)
	if err != nil {
		return err
	}
	return c.JSON(p)
}

func (h *Handler) timeline(c *fiber.Ctx) error {
	t, err := h.svc.Timeline(h.ctx(c), c.Params("orderNo"))
	if err != nil {
		return err
	}
	return c.JSON(t)
}

func (h *Handler) reportIssue(c *fiber.Ctx) error {
	var in usecase.ReportIssueInput
	if err := parseBody(c, &in); err != nil {
		return err
	}
	r, err := h.svc.ReportIssue(h.ctx(c), c.Params("orderNo"), c.Get("X-User-ID"), in)
	if err != nil {
		return err
	}
	return c.Status(fiber.StatusAccepted).JSON(r)
}

func (h *Handler) resolve(c *fiber.Ctx) error {
	var in usecase.ResolveInput
	if err := parseBody(c, &in); err != nil {
		return err
	}
	o, err := h.svc.Resolve(h.ctx(c), c.Params("orderNo"), c.Get("X-User-ID"), in)
	if err != nil {
		return err
	}
	return c.JSON(o)
}

// ---- webhook

func (h *Handler) paymentWebhook(c *fiber.Ctx) error {
	body := append([]byte(nil), c.Body()...) // copy: fiber reuses buffers
	r, err := h.svc.HandlePaymentWebhook(h.ctx(c), body, c.Get("X-Signature"))
	if err != nil {
		return err
	}
	return c.JSON(r)
}

// ---- bot

type botReq struct {
	BotID       string `json:"bot_id"`
	ProviderRef string `json:"provider_ref"`
	ErrorCode   string `json:"error_code"`
	Message     string `json:"message"`
}

func (h *Handler) botClaim(c *fiber.Ctx) error {
	var in botReq
	if err := parseBody(c, &in); err != nil {
		return err
	}
	o, err := h.svc.ClaimNext(h.ctx(c), in.BotID)
	if err != nil {
		return err
	}
	if o == nil {
		return c.SendStatus(fiber.StatusNoContent)
	}
	return c.JSON(o)
}

func (h *Handler) botHeartbeat(c *fiber.Ctx) error {
	var in botReq
	if err := parseBody(c, &in); err != nil {
		return err
	}
	if err := h.svc.Heartbeat(h.ctx(c), c.Params("orderNo"), in.BotID); err != nil {
		return err
	}
	return c.JSON(fiber.Map{"ok": true})
}

func (h *Handler) botComplete(c *fiber.Ctx) error {
	var in botReq
	if err := parseBody(c, &in); err != nil {
		return err
	}
	o, err := h.svc.Complete(h.ctx(c), c.Params("orderNo"), in.BotID, in.ProviderRef)
	if err != nil {
		return err
	}
	return c.JSON(o)
}

func (h *Handler) botFail(c *fiber.Ctx) error {
	var in botReq
	if err := parseBody(c, &in); err != nil {
		return err
	}
	o, err := h.svc.ReportFailure(h.ctx(c), c.Params("orderNo"), in.BotID, in.ErrorCode, in.Message)
	if err != nil {
		return err
	}
	return c.JSON(o)
}

func (h *Handler) runMaintenance(c *fiber.Ctx) error {
	return c.JSON(h.svc.RunMaintenance(h.ctx(c)))
}
