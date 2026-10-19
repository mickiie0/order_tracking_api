// Package usecase holds application business rules (order lifecycle, bot
// coordination, webhook handling, background maintenance).
package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/inblack/order-tracking/internal/apperror"
	"github.com/inblack/order-tracking/internal/domain"
)

type Config struct {
	PaymentTTL       time.Duration // PENDING_PAYMENT -> EXPIRED after this
	LeaseTimeout     time.Duration // bot heartbeat lease
	QueueSLA         time.Duration // QUEUED longer than this -> alert
	ReportDelayAfter time.Duration // customer report escalates QUEUED -> DELAYED after this
	BackoffBase      time.Duration
	BackoffMax       time.Duration
	MaxRetries       int
	WebhookSecret    string
}

func DefaultConfig() Config {
	return Config{
		PaymentTTL:       15 * time.Minute,
		LeaseTimeout:     3 * time.Minute,
		QueueSLA:         10 * time.Minute,
		ReportDelayAfter: 5 * time.Minute,
		BackoffBase:      30 * time.Second,
		BackoffMax:       30 * time.Minute,
		MaxRetries:       3,
		WebhookSecret:    "dev-webhook-secret",
	}
}

type OrderService struct {
	repo    domain.Repository
	gateway domain.PaymentGateway
	cfg     Config
	log     *slog.Logger
	Now     func() time.Time
}

func NewOrderService(repo domain.Repository, gw domain.PaymentGateway, cfg Config, log *slog.Logger) *OrderService {
	return &OrderService{repo: repo, gateway: gw, cfg: cfg, log: log, Now: func() time.Time { return time.Now().UTC() }}
}

func (s *OrderService) Config() Config { return s.cfg }

// Backoff returns base * 2^(attempt-1), capped. attempt starts at 1.
func Backoff(base, max time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := time.Duration(float64(base) * math.Pow(2, float64(attempt-1)))
	if d > max || d <= 0 {
		return max
	}
	return d
}

func toJSON(m map[string]any) string {
	if len(m) == 0 {
		return "{}"
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func mapRepoErr(err error, orderNo string) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrNotFound):
		return apperror.OrderNotFound(orderNo)
	case errors.Is(err, domain.ErrVersionConflict):
		return apperror.ConcurrentModification()
	}
	return err
}

// transition is the ONLY way an order changes status: validate against the
// state machine, CAS update on version, then append to the timeline - all in
// the caller's transaction.
func (s *OrderService) transition(ctx context.Context, tx domain.Repository, o *domain.Order,
	to domain.OrderStatus, reason string, meta map[string]any, extra map[string]any) error {

	from := o.Status
	if !domain.CanTransition(from, to) {
		return apperror.InvalidTransition(string(from), string(to))
	}
	upd := map[string]any{"status": to}
	for k, v := range extra {
		upd[k] = v
	}
	if err := tx.UpdateCAS(ctx, o, upd); err != nil {
		return mapRepoErr(err, o.OrderNo)
	}
	if err := tx.AddTimeline(ctx, &domain.OrderTimeline{
		OrderID: o.ID, FromStatus: string(from), ToStatus: string(to),
		Reason: reason, Metadata: toJSON(meta), CreatedAt: s.Now(),
	}); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "order status changed", "orderNo", o.OrderNo,
		"from", from, "to", to, "reason", reason, "version", o.Version)
	return nil
}

// note appends a timeline entry without changing status.
func (s *OrderService) note(ctx context.Context, tx domain.Repository, o *domain.Order, reason string, meta map[string]any) error {
	s.log.InfoContext(ctx, "order note", "orderNo", o.OrderNo, "status", o.Status, "reason", reason)
	return tx.AddTimeline(ctx, &domain.OrderTimeline{
		OrderID: o.ID, FromStatus: string(o.Status), ToStatus: string(o.Status),
		Reason: reason, Metadata: toJSON(meta), CreatedAt: s.Now(),
	})
}

// ------------------------------------------------------------------ Create

type CreateOrderInput struct {
	UserID  string  `json:"user_id"`
	GameUID string  `json:"game_uid"`
	Amount  float64 `json:"amount"`
}

func (in *CreateOrderInput) validate() error {
	d := map[string]string{}
	in.UserID, in.GameUID = strings.TrimSpace(in.UserID), strings.TrimSpace(in.GameUID)
	if in.UserID == "" || len(in.UserID) > 64 {
		d["user_id"] = "required, max 64 chars"
	}
	if in.GameUID == "" || len(in.GameUID) > 64 {
		d["game_uid"] = "required, max 64 chars"
	}
	if in.Amount <= 0 || in.Amount > 1_000_000 {
		d["amount"] = "must be > 0 and <= 1,000,000"
	}
	if len(d) > 0 {
		return apperror.Validation(d)
	}
	return nil
}

// CreateOrder returns (order, replayed, error). replayed=true means the
// Idempotency-Key was seen before and the original order is returned.
func (s *OrderService) CreateOrder(ctx context.Context, key string, in CreateOrderInput) (*domain.Order, bool, error) {
	key = strings.TrimSpace(key)
	if key == "" || len(key) > 128 {
		return nil, false, apperror.New(http.StatusBadRequest, "IDEMPOTENCY_KEY_REQUIRED",
			"Idempotency-Key header is required (max 128 chars)", nil)
	}
	if err := in.validate(); err != nil {
		return nil, false, err
	}
	replay := func(o *domain.Order) (*domain.Order, bool, error) {
		if o.UserID != in.UserID || o.GameUID != in.GameUID || o.Amount != in.Amount {
			return nil, false, apperror.New(http.StatusUnprocessableEntity, "IDEMPOTENCY_KEY_REUSED",
				"Idempotency-Key was already used with a different payload", nil)
		}
		s.log.InfoContext(ctx, "idempotent replay", "orderNo", o.OrderNo)
		return o, true, nil
	}
	if o, err := s.repo.FindByIdempotencyKey(ctx, key); err == nil {
		return replay(o)
	} else if !errors.Is(err, domain.ErrNotFound) {
		return nil, false, err
	}

	seq, err := s.repo.NextOrderSeq(ctx)
	if err != nil {
		return nil, false, err
	}
	now := s.Now()
	o := &domain.Order{
		OrderNo:        fmt.Sprintf("ORD-%s-%06d", now.Format("20060102"), seq),
		Status:         domain.StatusPendingPayment,
		UserID:         in.UserID,
		GameUID:        in.GameUID,
		Amount:         in.Amount,
		Version:        1,
		MaxRetries:     s.cfg.MaxRetries,
		ExpiresAt:      now.Add(s.cfg.PaymentTTL),
		IdempotencyKey: key,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	err = s.repo.WithTx(ctx, func(tx domain.Repository) error {
		if err := tx.Create(ctx, o); err != nil {
			return err
		}
		return tx.AddTimeline(ctx, &domain.OrderTimeline{
			OrderID: o.ID, FromStatus: "", ToStatus: string(o.Status),
			Reason: "ORDER_CREATED", Metadata: toJSON(map[string]any{"expires_at": o.ExpiresAt}), CreatedAt: now,
		})
	})
	if errors.Is(err, domain.ErrDuplicate) {
		// Lost the race against a concurrent request with the same key.
		existing, ferr := s.repo.FindByIdempotencyKey(ctx, key)
		if ferr != nil {
			return nil, false, ferr
		}
		return replay(existing)
	}
	if err != nil {
		return nil, false, err
	}
	s.log.InfoContext(ctx, "order created", "orderNo", o.OrderNo, "userId", o.UserID, "amount", o.Amount)
	return o, false, nil
}

// ------------------------------------------------------------------ Queries

func (s *OrderService) GetOrder(ctx context.Context, orderNo string) (*domain.Order, error) {
	if !domain.ValidOrderNo(orderNo) {
		return nil, apperror.InvalidOrderNo(orderNo)
	}
	o, err := s.repo.FindByOrderNo(ctx, orderNo, false)
	return o, mapRepoErr(err, orderNo)
}

type Page struct {
	Data       []domain.Order `json:"data"`
	Page       int            `json:"page"`
	Limit      int            `json:"limit"`
	Total      int64          `json:"total"`
	TotalPages int64          `json:"total_pages"`
}

func (s *OrderService) ListOrders(ctx context.Context, tab, userID string, page, limit int) (*Page, error) {
	d := map[string]string{}
	var statuses []domain.OrderStatus
	switch tab {
	case "", "current":
		statuses = domain.CurrentStatuses
	case "history":
		statuses = domain.HistoryStatuses
	default:
		d["tab"] = "must be current or history"
	}
	if page < 1 {
		d["page"] = "must be >= 1"
	}
	if limit < 1 || limit > 100 {
		d["limit"] = "must be between 1 and 100"
	}
	if len(d) > 0 {
		return nil, apperror.Validation(d)
	}
	rows, total, err := s.repo.List(ctx, domain.ListFilter{UserID: userID, Statuses: statuses, Page: page, Limit: limit})
	if err != nil {
		return nil, err
	}
	if rows == nil {
		rows = []domain.Order{}
	}
	return &Page{Data: rows, Page: page, Limit: limit, Total: total,
		TotalPages: (total + int64(limit) - 1) / int64(limit)}, nil
}

type TimelineView struct {
	OrderNo string                 `json:"order_no"`
	Status  domain.OrderStatus     `json:"status"`
	Events  []domain.OrderTimeline `json:"events"`
}

func (s *OrderService) Timeline(ctx context.Context, orderNo string) (*TimelineView, error) {
	o, err := s.GetOrder(ctx, orderNo)
	if err != nil {
		return nil, err
	}
	ev, err := s.repo.ListTimeline(ctx, o.ID)
	if err != nil {
		return nil, err
	}
	return &TimelineView{OrderNo: o.OrderNo, Status: o.Status, Events: ev}, nil
}

// ------------------------------------------------------------------ Customer actions

type ReportIssueInput struct {
	Reason      string `json:"reason"`
	Description string `json:"description"`
}

type ReportIssueResult struct {
	Order     *domain.Order `json:"order"`
	Escalated bool          `json:"escalated"`
	Message   string        `json:"message"`
}

func checkOwner(o *domain.Order, userID string) error {
	if userID != "" && o.UserID != userID {
		return apperror.Forbidden()
	}
	return nil
}

// ReportIssue always records the complaint on the timeline. It escalates to
// DELAYED only when the order is QUEUED longer than ReportDelayAfter: an
// IN_PROGRESS order is owned by a bot lease and must not be yanked away
// (that could cause double payment) - the lease reaper handles it.
func (s *OrderService) ReportIssue(ctx context.Context, orderNo, userID string, in ReportIssueInput) (*ReportIssueResult, error) {
	if !domain.ValidOrderNo(orderNo) {
		return nil, apperror.InvalidOrderNo(orderNo)
	}
	in.Reason = strings.TrimSpace(in.Reason)
	if in.Reason == "" || len(in.Reason) > 200 || len(in.Description) > 2000 {
		return nil, apperror.Validation(map[string]string{"reason": "required, max 200 chars; description max 2000"})
	}
	res := &ReportIssueResult{}
	err := s.repo.WithTx(ctx, func(tx domain.Repository) error {
		o, err := tx.FindByOrderNo(ctx, orderNo, true)
		if err != nil {
			return mapRepoErr(err, orderNo)
		}
		if err := checkOwner(o, userID); err != nil {
			return err
		}
		meta := map[string]any{"reason": in.Reason, "description": in.Description, "source": "customer"}
		if err := s.note(ctx, tx, o, "CUSTOMER_REPORTED_ISSUE: "+in.Reason, meta); err != nil {
			return err
		}
		res.Order = o
		if o.Status == domain.StatusQueued && o.QueuedAt != nil && s.Now().Sub(*o.QueuedAt) >= s.cfg.ReportDelayAfter {
			if err := s.transition(ctx, tx, o, domain.StatusDelayed, "ESCALATED_BY_CUSTOMER_REPORT", meta, nil); err != nil {
				return err
			}
			s.log.ErrorContext(ctx, "ALERT order escalated by customer", "orderNo", o.OrderNo, "alert", true)
			res.Escalated = true
			res.Message = "order escalated to DELAYED; support team notified"
			return nil
		}
		res.Message = "issue recorded; order is " + string(o.Status)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

type ResolveInput struct {
	Action  string `json:"action"` // RETRY | REFUND
	GameUID string `json:"game_uid"`
}

// Resolve lets a customer fix a FAILED/DELAYED order: correct the Game UID and
// re-queue it, or request a refund.
func (s *OrderService) Resolve(ctx context.Context, orderNo, userID string, in ResolveInput) (*domain.Order, error) {
	if !domain.ValidOrderNo(orderNo) {
		return nil, apperror.InvalidOrderNo(orderNo)
	}
	in.Action = strings.ToUpper(strings.TrimSpace(in.Action))
	in.GameUID = strings.TrimSpace(in.GameUID)
	if in.Action != "RETRY" && in.Action != "REFUND" {
		return nil, apperror.Validation(map[string]string{"action": "must be RETRY or REFUND"})
	}
	if len(in.GameUID) > 64 {
		return nil, apperror.Validation(map[string]string{"game_uid": "max 64 chars"})
	}
	var out *domain.Order
	err := s.repo.WithTx(ctx, func(tx domain.Repository) error {
		o, err := tx.FindByOrderNo(ctx, orderNo, true)
		if err != nil {
			return mapRepoErr(err, orderNo)
		}
		if err := checkOwner(o, userID); err != nil {
			return err
		}
		if o.Status != domain.StatusFailed && o.Status != domain.StatusDelayed {
			return apperror.New(http.StatusConflict, "ORDER_NOT_RESOLVABLE",
				"only FAILED or DELAYED orders can be resolved", map[string]string{"status": string(o.Status)})
		}
		out = o
		if in.Action == "REFUND" {
			return s.refund(ctx, tx, o, "CUSTOMER_REQUESTED_REFUND")
		}
		meta := map[string]any{"old_game_uid": o.GameUID}
		extra := map[string]any{
			"retry_count": 0, "next_attempt_at": nil, "locked_by": nil, "locked_at": nil,
			"last_error": "", "queued_at": s.Now(),
		}
		if in.GameUID != "" {
			extra["game_uid"] = in.GameUID
			meta["new_game_uid"] = in.GameUID
		}
		return s.transition(ctx, tx, o, domain.StatusQueued, "CUSTOMER_RESOLVED_RETRY", meta, extra)
	})
	return out, err
}

func (s *OrderService) refund(ctx context.Context, tx domain.Repository, o *domain.Order, reason string) error {
	ref, err := s.gateway.Refund(ctx, o.OrderNo, o.Amount)
	if err != nil {
		return fmt.Errorf("refund %s: %w", o.OrderNo, err)
	}
	return s.transition(ctx, tx, o, domain.StatusRefunded, reason,
		map[string]any{"refund_ref": ref, "amount": o.Amount},
		map[string]any{"locked_by": nil, "locked_at": nil})
}
