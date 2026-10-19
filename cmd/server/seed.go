package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/inblack/order-tracking/internal/domain"
	"gorm.io/gorm"
)

// seed inserts deterministic demo orders (only when the table is empty).
// Order numbers use the 9xxxxx range so they never clash with the sequence.
func seed(db *gorm.DB, log *slog.Logger) error {
	var n int64
	if err := db.Model(&domain.Order{}).Count(&n).Error; err != nil || n > 0 {
		return err
	}
	now := time.Now().UTC()
	ago := func(d time.Duration) *time.Time { t := now.Add(-d); return &t }
	str := func(s string) *string { return &s }

	type row struct {
		no, uid, reason string
		status          domain.OrderStatus
		amount          float64
		mut             func(o *domain.Order)
	}
	rows := []row{
		{"ORD-20260101-900001", "123456789", "seed: waiting for payment", domain.StatusPendingPayment, 100, nil},
		{"ORD-20260101-900002", "223456789", "seed: QR already past expiry (scheduler will EXPIRE)", domain.StatusPendingPayment, 150,
			func(o *domain.Order) { o.ExpiresAt = now.Add(-time.Minute) }},
		{"ORD-20260101-900003", "323456789", "seed: paid, waiting for bot", domain.StatusQueued, 200,
			func(o *domain.Order) { o.QueuedAt = ago(time.Minute) }},
		{"ORD-20260101-900004", "HANG-0004", "seed: claimed by dead bot, stale lease (reaper will reclaim)", domain.StatusInProgress, 250,
			func(o *domain.Order) { o.LockedBy = str("bot-dead"); o.LockedAt = ago(10 * time.Minute); o.QueuedAt = ago(12 * time.Minute) }},
		{"ORD-20260101-900005", "523456789", "seed: completed", domain.StatusCompleted, 300, nil},
		{"ORD-20260101-900006", "INVALID-0006", "seed: PERMANENT_ERROR: INVALID_UID", domain.StatusFailed, 350,
			func(o *domain.Order) { o.LastError = "INVALID_UID: game uid does not exist" }},
		{"ORD-20260101-900007", "DOWN-0007", "seed: MAX_RETRIES_EXCEEDED: PROVIDER_UNAVAILABLE", domain.StatusDelayed, 400,
			func(o *domain.Order) { o.RetryCount = 3; o.LastError = "PROVIDER_UNAVAILABLE" }},
		{"ORD-20260101-900008", "823456789", "seed: payment timeout", domain.StatusExpired, 450,
			func(o *domain.Order) { o.ExpiresAt = now.Add(-time.Hour) }},
		{"ORD-20260101-900009", "923456789", "seed: refunded", domain.StatusRefunded, 500, nil},
		{"ORD-20260101-900010", "103456789", "seed: queued for a long time (SLA alert, report-issue escalation)", domain.StatusQueued, 550,
			func(o *domain.Order) { o.QueuedAt = ago(30 * time.Minute) }},
	}
	return db.Transaction(func(tx *gorm.DB) error {
		for i, r := range rows {
			o := &domain.Order{
				OrderNo: r.no, Status: r.status, UserID: "user-001", GameUID: r.uid, Amount: r.amount,
				Version: 1, MaxRetries: 3, ExpiresAt: now.Add(15 * time.Minute),
				IdempotencyKey: fmt.Sprintf("seed-%03d", i+1),
				CreatedAt:      now.Add(-time.Duration(len(rows)-i) * time.Minute), UpdatedAt: now,
			}
			if r.mut != nil {
				r.mut(o)
			}
			if err := tx.Create(o).Error; err != nil {
				return err
			}
			if err := tx.Create(&domain.OrderTimeline{OrderID: o.ID, FromStatus: "", ToStatus: string(o.Status),
				Reason: r.reason, Metadata: `{"seed":true}`, CreatedAt: o.CreatedAt}).Error; err != nil {
				return err
			}
		}
		log.Info("seed data inserted", "orders", len(rows))
		return nil
	})
}
