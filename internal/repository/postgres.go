// Package repository is the PostgreSQL/GORM implementation of domain.Repository.
package repository

import (
	"context"
	"errors"
	"time"

	"github.com/inblack/order-tracking/internal/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type PostgresRepo struct{ db *gorm.DB }

func NewPostgres(db *gorm.DB) *PostgresRepo { return &PostgresRepo{db: db} }

func (r *PostgresRepo) q(ctx context.Context) *gorm.DB { return r.db.WithContext(ctx) }

func translate(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, gorm.ErrRecordNotFound):
		return domain.ErrNotFound
	case errors.Is(err, gorm.ErrDuplicatedKey):
		return domain.ErrDuplicate
	}
	return err
}

func (r *PostgresRepo) WithTx(ctx context.Context, fn func(domain.Repository) error) error {
	return r.q(ctx).Transaction(func(tx *gorm.DB) error {
		return fn(&PostgresRepo{db: tx})
	})
}

func (r *PostgresRepo) NextOrderSeq(ctx context.Context) (int64, error) {
	var n int64
	err := r.q(ctx).Raw("SELECT nextval('order_no_seq')").Scan(&n).Error
	return n, err
}

func (r *PostgresRepo) Create(ctx context.Context, o *domain.Order) error {
	return translate(r.q(ctx).Create(o).Error)
}

func (r *PostgresRepo) FindByOrderNo(ctx context.Context, orderNo string, forUpdate bool) (*domain.Order, error) {
	q := r.q(ctx)
	if forUpdate {
		q = q.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var o domain.Order
	if err := q.Where("order_no = ?", orderNo).Take(&o).Error; err != nil {
		return nil, translate(err)
	}
	return &o, nil
}

func (r *PostgresRepo) FindByIdempotencyKey(ctx context.Context, key string) (*domain.Order, error) {
	var o domain.Order
	if err := r.q(ctx).Where("idempotency_key = ?", key).Take(&o).Error; err != nil {
		return nil, translate(err)
	}
	return &o, nil
}

func (r *PostgresRepo) List(ctx context.Context, f domain.ListFilter) ([]domain.Order, int64, error) {
	q := r.q(ctx).Model(&domain.Order{}).Where("status IN ?", f.Statuses)
	if f.UserID != "" {
		q = q.Where("user_id = ?", f.UserID)
	}
	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var out []domain.Order
	err := q.Order("created_at DESC, id DESC").Offset((f.Page - 1) * f.Limit).Limit(f.Limit).Find(&out).Error
	return out, total, err
}

func (r *PostgresRepo) UpdateCAS(ctx context.Context, o *domain.Order, updates map[string]any) error {
	updates["version"] = gorm.Expr("version + 1")
	updates["updated_at"] = time.Now().UTC()
	res := r.q(ctx).Model(&domain.Order{}).
		Where("id = ? AND version = ?", o.ID, o.Version).
		Updates(updates)
	if res.Error != nil {
		return translate(res.Error)
	}
	if res.RowsAffected == 0 {
		return domain.ErrVersionConflict
	}
	return r.q(ctx).Take(o, o.ID).Error
}

func (r *PostgresRepo) ClaimNext(ctx context.Context, now time.Time) (*domain.Order, error) {
	var o domain.Order
	err := r.q(ctx).
		Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
		Where("status = ? AND (next_attempt_at IS NULL OR next_attempt_at <= ?)", domain.StatusQueued, now).
		Order("created_at ASC, id ASC").
		Limit(1).
		Take(&o).Error
	if err != nil {
		return nil, translate(err)
	}
	return &o, nil
}

func (r *PostgresRepo) Heartbeat(ctx context.Context, orderNo, botID string, now time.Time) (bool, error) {
	res := r.q(ctx).Model(&domain.Order{}).
		Where("order_no = ? AND status = ? AND locked_by = ?", orderNo, domain.StatusInProgress, botID).
		Update("locked_at", now)
	return res.RowsAffected == 1, res.Error
}

func (r *PostgresRepo) FindStaleLeases(ctx context.Context, cutoff time.Time, limit int) ([]domain.Order, error) {
	var out []domain.Order
	err := r.q(ctx).Where("status = ? AND locked_at < ?", domain.StatusInProgress, cutoff).
		Order("locked_at ASC").Limit(limit).Find(&out).Error
	return out, err
}

func (r *PostgresRepo) FindExpiredPending(ctx context.Context, now time.Time, limit int) ([]domain.Order, error) {
	var out []domain.Order
	err := r.q(ctx).Where("status = ? AND expires_at < ?", domain.StatusPendingPayment, now).
		Order("expires_at ASC").Limit(limit).Find(&out).Error
	return out, err
}

func (r *PostgresRepo) FindStuckQueued(ctx context.Context, cutoff time.Time, limit int) ([]domain.Order, error) {
	var out []domain.Order
	err := r.q(ctx).Where("status = ? AND queued_at < ?", domain.StatusQueued, cutoff).
		Order("queued_at ASC").Limit(limit).Find(&out).Error
	return out, err
}

func (r *PostgresRepo) AddTimeline(ctx context.Context, t *domain.OrderTimeline) error {
	if t.Metadata == "" {
		t.Metadata = "{}"
	}
	return r.q(ctx).Create(t).Error
}

func (r *PostgresRepo) ListTimeline(ctx context.Context, orderID uint64) ([]domain.OrderTimeline, error) {
	var out []domain.OrderTimeline
	err := r.q(ctx).Where("order_id = ?", orderID).Order("created_at ASC, id ASC").Find(&out).Error
	return out, err
}

func (r *PostgresRepo) CreatePayment(ctx context.Context, p *domain.PaymentTransaction) error {
	return translate(r.q(ctx).Create(p).Error)
}

func (r *PostgresRepo) InsertWebhookEvent(ctx context.Context, e *domain.WebhookEvent) (bool, error) {
	res := r.q(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(e)
	return res.RowsAffected == 1, res.Error
}
