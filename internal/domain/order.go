// Package domain contains enterprise entities, the order state machine and
// the ports (interfaces) that outer layers must implement.
package domain

import (
	"regexp"
	"time"
)

type OrderStatus string

const (
	StatusPendingPayment OrderStatus = "PENDING_PAYMENT"
	StatusQueued         OrderStatus = "QUEUED"
	StatusInProgress     OrderStatus = "IN_PROGRESS"
	StatusCompleted      OrderStatus = "COMPLETED"
	StatusDelayed        OrderStatus = "DELAYED"
	StatusFailed         OrderStatus = "FAILED"
	StatusExpired        OrderStatus = "EXPIRED"
	StatusRefunded       OrderStatus = "REFUNDED"
)

// Tabs used by GET /orders?tab=...
var (
	CurrentStatuses = []OrderStatus{StatusPendingPayment, StatusQueued, StatusInProgress, StatusDelayed}
	HistoryStatuses = []OrderStatus{StatusCompleted, StatusFailed, StatusExpired, StatusRefunded}
)

// OrderNoPattern is the public format contract for order numbers.
// Generated format: ORD-YYYYMMDD-NNNNNN (NNNNNN comes from a DB sequence).
var OrderNoPattern = regexp.MustCompile(`^ORD-\d+-\d+$`)

func ValidOrderNo(s string) bool { return len(s) <= 40 && OrderNoPattern.MatchString(s) }

type Order struct {
	ID             uint64      `gorm:"primaryKey" json:"id"`
	OrderNo        string      `gorm:"size:40;not null;uniqueIndex" json:"order_no"`
	Status         OrderStatus `gorm:"size:32;not null;index:idx_orders_status_created,priority:1;index:idx_orders_user_status,priority:2" json:"status"`
	UserID         string      `gorm:"size:64;not null;index:idx_orders_user_status,priority:1" json:"user_id"`
	GameUID        string      `gorm:"size:64;not null" json:"game_uid"`
	Amount         float64     `gorm:"type:numeric(12,2);not null" json:"amount"`
	Version        int64       `gorm:"not null;default:1" json:"version"`
	LockedAt       *time.Time  `gorm:"index" json:"locked_at,omitempty"`
	LockedBy       *string     `gorm:"size:64" json:"locked_by,omitempty"`
	RetryCount     int         `gorm:"not null;default:0" json:"retry_count"`
	MaxRetries     int         `gorm:"not null;default:3" json:"max_retries"`
	NextAttemptAt  *time.Time  `json:"next_attempt_at,omitempty"`
	QueuedAt       *time.Time  `json:"queued_at,omitempty"`
	ExpiresAt      time.Time   `gorm:"not null;index" json:"expires_at"`
	LastError      string      `gorm:"size:512" json:"last_error,omitempty"`
	IdempotencyKey string      `gorm:"size:128;not null;uniqueIndex" json:"-"`
	CreatedAt      time.Time   `gorm:"not null;index:idx_orders_status_created,priority:2,sort:desc" json:"created_at"`
	UpdatedAt      time.Time   `gorm:"not null" json:"updated_at"`
}

func (Order) TableName() string { return "orders" }

type OrderTimeline struct {
	ID         uint64    `gorm:"primaryKey" json:"id"`
	OrderID    uint64    `gorm:"not null;index" json:"order_id"`
	FromStatus string    `gorm:"size:32" json:"from_status"`
	ToStatus   string    `gorm:"size:32;not null" json:"to_status"`
	Reason     string    `gorm:"size:512;not null" json:"reason"`
	Metadata   string    `gorm:"type:jsonb;not null;default:'{}'" json:"metadata"`
	CreatedAt  time.Time `gorm:"not null" json:"created_at"`
}

func (OrderTimeline) TableName() string { return "order_timelines" }

// PaymentTransaction: unique(order_id) is the last line of defence against
// double payment even if every application-level guard fails.
type PaymentTransaction struct {
	ID          uint64    `gorm:"primaryKey" json:"id"`
	OrderID     uint64    `gorm:"not null;uniqueIndex" json:"order_id"`
	BotID       string    `gorm:"size:64;not null" json:"bot_id"`
	Amount      float64   `gorm:"type:numeric(12,2);not null" json:"amount"`
	ProviderRef string    `gorm:"size:128;not null" json:"provider_ref"`
	CreatedAt   time.Time `gorm:"not null" json:"created_at"`
}

func (PaymentTransaction) TableName() string { return "payment_transactions" }

// WebhookEvent: primary key on event_id gives exactly-once processing.
type WebhookEvent struct {
	EventID    string    `gorm:"primaryKey;size:128" json:"event_id"`
	OrderNo    string    `gorm:"size:40;not null;index" json:"order_no"`
	Type       string    `gorm:"size:64;not null" json:"type"`
	Payload    string    `gorm:"type:jsonb;not null" json:"payload"`
	Result     string    `gorm:"size:64" json:"result"`
	ReceivedAt time.Time `gorm:"not null" json:"received_at"`
}

func (WebhookEvent) TableName() string { return "webhook_events" }
