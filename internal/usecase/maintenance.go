package usecase

import (
	"context"
	"errors"

	"github.com/inblack/order-tracking/internal/apperror"
	"github.com/inblack/order-tracking/internal/domain"
)

const batchSize = 100

// MaintenanceResult is returned for observability / tests.
type MaintenanceResult struct {
	Expired      int `json:"expired"`
	LeasesReaped int `json:"leases_reaped"`
	StuckQueued  int `json:"stuck_queued"`
}

// RunMaintenance executes one tick of every background job. Every job
// re-reads the row FOR UPDATE and re-checks its condition, so running many
// replicas concurrently is safe.
func (s *OrderService) RunMaintenance(ctx context.Context) MaintenanceResult {
	var r MaintenanceResult
	r.Expired = s.ExpirePending(ctx)
	r.LeasesReaped = s.ReapStaleLeases(ctx)
	r.StuckQueued = s.AlertStuckQueued(ctx)
	return r
}

func benign(err error) bool {
	var ae *apperror.Error
	return errors.As(err, &ae) // state already changed by someone else
}

// ExpirePending: PENDING_PAYMENT past expires_at (QR expired) -> EXPIRED.
func (s *OrderService) ExpirePending(ctx context.Context) int {
	now := s.Now()
	rows, err := s.repo.FindExpiredPending(ctx, now, batchSize)
	if err != nil {
		s.log.ErrorContext(ctx, "expire scan failed", "err", err)
		return 0
	}
	n := 0
	for _, c := range rows {
		err := s.repo.WithTx(ctx, func(tx domain.Repository) error {
			o, err := tx.FindByOrderNo(ctx, c.OrderNo, true)
			if err != nil {
				return err
			}
			if o.Status != domain.StatusPendingPayment || !o.ExpiresAt.Before(now) {
				return nil // paid meanwhile
			}
			n++
			return s.transition(ctx, tx, o, domain.StatusExpired, "PAYMENT_TIMEOUT_QR_EXPIRED",
				map[string]any{"expires_at": o.ExpiresAt}, nil)
		})
		if err != nil && !benign(err) {
			s.log.ErrorContext(ctx, "expire failed", "orderNo", c.OrderNo, "err", err)
		}
	}
	return n
}

// ReapStaleLeases: IN_PROGRESS whose bot stopped heart-beating for
// LeaseTimeout -> treated as a transient failure (retry w/ backoff, then DELAYED).
func (s *OrderService) ReapStaleLeases(ctx context.Context) int {
	cutoff := s.Now().Add(-s.cfg.LeaseTimeout)
	rows, err := s.repo.FindStaleLeases(ctx, cutoff, batchSize)
	if err != nil {
		s.log.ErrorContext(ctx, "lease scan failed", "err", err)
		return 0
	}
	n := 0
	for _, c := range rows {
		err := s.repo.WithTx(ctx, func(tx domain.Repository) error {
			o, err := tx.FindByOrderNo(ctx, c.OrderNo, true)
			if err != nil {
				return err
			}
			if o.Status != domain.StatusInProgress || o.LockedAt == nil || !o.LockedAt.Before(cutoff) {
				return nil // bot heart-beated or finished meanwhile
			}
			bot := ""
			if o.LockedBy != nil {
				bot = *o.LockedBy
			}
			s.log.WarnContext(ctx, "bot lease expired, reclaiming order", "orderNo", o.OrderNo, "botId", bot, "lockedAt", o.LockedAt)
			n++
			return s.handleFailure(ctx, tx, o, "BOT_LEASE_EXPIRED", "bot did not heartbeat within lease timeout",
				map[string]any{"bot_id": bot, "locked_at": o.LockedAt})
		})
		if err != nil && !benign(err) {
			s.log.ErrorContext(ctx, "reap failed", "orderNo", c.OrderNo, "err", err)
		}
	}
	return n
}

// AlertStuckQueued raises an alert for orders waiting in the queue beyond SLA
// (e.g. all bots down). It does not change state - humans/ops decide.
func (s *OrderService) AlertStuckQueued(ctx context.Context) int {
	rows, err := s.repo.FindStuckQueued(ctx, s.Now().Add(-s.cfg.QueueSLA), batchSize)
	if err != nil {
		s.log.ErrorContext(ctx, "stuck scan failed", "err", err)
		return 0
	}
	for _, o := range rows {
		s.log.ErrorContext(ctx, "ALERT order queued beyond SLA", "orderNo", o.OrderNo,
			"queuedAt", o.QueuedAt, "sla", s.cfg.QueueSLA.String(), "alert", true)
	}
	return len(rows)
}
