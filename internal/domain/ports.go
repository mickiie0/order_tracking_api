package domain

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Repository-level sentinel errors (infrastructure translates DB errors to these).
var (
	ErrNotFound        = errors.New("not found")
	ErrDuplicate       = errors.New("duplicate")
	ErrVersionConflict = errors.New("version conflict")
)

type ListFilter struct {
	UserID   string
	Statuses []OrderStatus
	Page     int
	Limit    int
}

// Repository is the persistence port.
type Repository interface {
	WithTx(ctx context.Context, fn func(tx Repository) error) error

	NextOrderSeq(ctx context.Context) (int64, error)
	Create(ctx context.Context, o *Order) error
	FindByOrderNo(ctx context.Context, orderNo string, forUpdate bool) (*Order, error)
	FindByIdempotencyKey(ctx context.Context, key string) (*Order, error)
	List(ctx context.Context, f ListFilter) ([]Order, int64, error)
	// UpdateCAS performs an optimistic-locking update (WHERE version = o.Version)
	// and reloads o. Returns ErrVersionConflict if someone else won.
	UpdateCAS(ctx context.Context, o *Order, updates map[string]any) error
	// ClaimNext locks the next claimable QUEUED order with FOR UPDATE SKIP LOCKED.
	ClaimNext(ctx context.Context, now time.Time) (*Order, error)
	Heartbeat(ctx context.Context, orderNo, botID string, now time.Time) (bool, error)

	FindStaleLeases(ctx context.Context, cutoff time.Time, limit int) ([]Order, error)
	FindExpiredPending(ctx context.Context, now time.Time, limit int) ([]Order, error)
	FindStuckQueued(ctx context.Context, cutoff time.Time, limit int) ([]Order, error)

	AddTimeline(ctx context.Context, t *OrderTimeline) error
	ListTimeline(ctx context.Context, orderID uint64) ([]OrderTimeline, error)

	CreatePayment(ctx context.Context, p *PaymentTransaction) error
	// InsertWebhookEvent returns false when event_id was already processed.
	InsertWebhookEvent(ctx context.Context, e *WebhookEvent) (bool, error)
}

// ---- Game provider port ----

type ErrorKind int

const (
	ErrorTransient ErrorKind = iota // network / provider down -> retry with backoff
	ErrorPermanent                  // wrong UID etc. -> FAILED immediately
)

// permanentCodes: errors where retrying can never succeed.
var permanentCodes = map[string]bool{
	"INVALID_UID":     true,
	"UID_NOT_FOUND":   true,
	"ACCOUNT_BANNED":  true,
	"REGION_MISMATCH": true,
	"INVALID_PACKAGE": true,
}

// ClassifyErrorCode defaults to transient: unknown errors get a bounded number
// of retries and then escalate to DELAYED for a human, which is safer than
// failing an order that might have succeeded on retry.
func ClassifyErrorCode(code string) ErrorKind {
	if permanentCodes[code] {
		return ErrorPermanent
	}
	return ErrorTransient
}

type ProviderError struct {
	Code    string
	Message string
}

func (e *ProviderError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

type GameProvider interface {
	TopUp(ctx context.Context, o *Order) (providerRef string, err error)
}

type PaymentGateway interface {
	Refund(ctx context.Context, orderNo string, amount float64) (refundRef string, err error)
}
