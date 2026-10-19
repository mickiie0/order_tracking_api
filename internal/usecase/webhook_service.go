package usecase

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/inblack/order-tracking/internal/apperror"
	"github.com/inblack/order-tracking/internal/domain"
)

const (
	WebhookPaymentSucceeded = "payment.succeeded"
	WebhookPaymentFailed    = "payment.failed"
)

type PaymentWebhook struct {
	EventID    string    `json:"event_id"`
	OrderNo    string    `json:"order_no"`
	Type       string    `json:"type"`
	Amount     float64   `json:"amount"`
	OccurredAt time.Time `json:"occurred_at"`
}

type WebhookResult struct {
	EventID string `json:"event_id"`
	Result  string `json:"result"`
	Status  string `json:"order_status,omitempty"`
}

// Sign returns hex(HMAC-SHA256(secret, body)).
func Sign(secret string, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

// VerifySignature uses constant-time comparison to avoid timing attacks.
func VerifySignature(secret string, body []byte, sig string) bool {
	sig = strings.TrimPrefix(strings.TrimSpace(sig), "sha256=")
	got, err := hex.DecodeString(sig)
	if err != nil {
		return false
	}
	want, _ := hex.DecodeString(Sign(secret, body))
	return hmac.Equal(got, want)
}

// HandlePaymentWebhook processes bank callbacks that may be late, duplicated
// or out of order:
//  1. verify HMAC signature
//  2. INSERT webhook_events ON CONFLICT DO NOTHING (event_id idempotency) -
//     in the same TX as the state change, so "recorded" == "applied".
//  3. lock the order row and decide based on the CURRENT state, never on
//     arrival order.
func (s *OrderService) HandlePaymentWebhook(ctx context.Context, body []byte, signature string) (*WebhookResult, error) {
	if !VerifySignature(s.cfg.WebhookSecret, body, signature) {
		s.log.WarnContext(ctx, "webhook signature rejected")
		return nil, apperror.New(http.StatusUnauthorized, "INVALID_SIGNATURE", "webhook signature verification failed", nil)
	}
	var ev PaymentWebhook
	if err := json.Unmarshal(body, &ev); err != nil {
		return nil, apperror.Validation(map[string]string{"body": "invalid JSON"})
	}
	d := map[string]string{}
	if ev.EventID == "" || len(ev.EventID) > 128 {
		d["event_id"] = "required, max 128 chars"
	}
	if !domain.ValidOrderNo(ev.OrderNo) {
		d["order_no"] = "invalid format"
	}
	if ev.Type != WebhookPaymentSucceeded && ev.Type != WebhookPaymentFailed {
		d["type"] = "must be payment.succeeded or payment.failed"
	}
	if len(d) > 0 {
		return nil, apperror.Validation(d)
	}

	res := &WebhookResult{EventID: ev.EventID}
	err := s.repo.WithTx(ctx, func(tx domain.Repository) error {
		rec := &domain.WebhookEvent{EventID: ev.EventID, OrderNo: ev.OrderNo, Type: ev.Type,
			Payload: string(body), ReceivedAt: s.Now()}
		inserted, err := tx.InsertWebhookEvent(ctx, rec)
		if err != nil {
			return err
		}
		if !inserted {
			res.Result = "DUPLICATE_IGNORED"
			s.log.InfoContext(ctx, "duplicate webhook ignored", "orderNo", ev.OrderNo, "eventId", ev.EventID)
			return nil
		}
		o, err := tx.FindByOrderNo(ctx, ev.OrderNo, true)
		if err != nil {
			return mapRepoErr(err, ev.OrderNo) // rollback -> bank will retry
		}
		meta := map[string]any{"event_id": ev.EventID, "type": ev.Type, "amount": ev.Amount, "occurred_at": ev.OccurredAt}

		if ev.Type == WebhookPaymentFailed {
			res.Result = "NOTED"
			return s.note(ctx, tx, o, "PAYMENT_FAILED_WEBHOOK", meta)
		}
		if math.Abs(ev.Amount-o.Amount) > 0.001 {
			res.Result = "AMOUNT_MISMATCH"
			s.log.ErrorContext(ctx, "ALERT webhook amount mismatch", "orderNo", o.OrderNo,
				"expected", o.Amount, "got", ev.Amount, "alert", true)
			return s.note(ctx, tx, o, "PAYMENT_AMOUNT_MISMATCH", meta)
		}
		switch o.Status {
		case domain.StatusPendingPayment:
			res.Result = "QUEUED"
			err = s.transition(ctx, tx, o, domain.StatusQueued, "PAYMENT_CONFIRMED", meta,
				map[string]any{"queued_at": s.Now()})
		case domain.StatusExpired:
			// Customer paid after the QR expired: money is in, but the order is
			// dead -> refund automatically instead of silently completing.
			res.Result = "AUTO_REFUNDED"
			s.log.WarnContext(ctx, "late payment on expired order, auto refund", "orderNo", o.OrderNo)
			err = s.refund(ctx, tx, o, "LATE_PAYMENT_AUTO_REFUND")
		default:
			// Already paid (another event_id for the same payment, or out-of-order).
			res.Result = "IGNORED_STATE_" + string(o.Status)
			err = s.note(ctx, tx, o, "PAYMENT_WEBHOOK_IGNORED_ALREADY_PAID", meta)
		}
		if err != nil {
			return err
		}
		res.Status = string(o.Status)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}
