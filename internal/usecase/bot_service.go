package usecase

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/inblack/order-tracking/internal/apperror"
	"github.com/inblack/order-tracking/internal/domain"
)

// ClaimNext atomically hands the oldest claimable QUEUED order to botID.
// SELECT ... FOR UPDATE SKIP LOCKED guarantees two bots never receive the same
// row, and the CAS on version is a second, independent guard.
// Returns (nil, nil) when there is nothing to do.
func (s *OrderService) ClaimNext(ctx context.Context, botID string) (*domain.Order, error) {
	botID = strings.TrimSpace(botID)
	if botID == "" || len(botID) > 64 {
		return nil, apperror.Validation(map[string]string{"bot_id": "required, max 64 chars"})
	}
	var claimed *domain.Order
	err := s.repo.WithTx(ctx, func(tx domain.Repository) error {
		now := s.Now()
		o, err := tx.ClaimNext(ctx, now)
		if errors.Is(err, domain.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := s.transition(ctx, tx, o, domain.StatusInProgress, "CLAIMED_BY_BOT",
			map[string]any{"bot_id": botID, "attempt": o.RetryCount + 1},
			map[string]any{"locked_by": botID, "locked_at": now}); err != nil {
			return err
		}
		claimed = o
		return nil
	})
	return claimed, err
}

// Heartbeat extends the lease. Fails with LEASE_LOST if the reaper already
// took the order back (the bot MUST stop working on it).
func (s *OrderService) Heartbeat(ctx context.Context, orderNo, botID string) error {
	if !domain.ValidOrderNo(orderNo) {
		return apperror.InvalidOrderNo(orderNo)
	}
	ok, err := s.repo.Heartbeat(ctx, orderNo, botID, s.Now())
	if err != nil {
		return err
	}
	if !ok {
		return apperror.LeaseLost(orderNo)
	}
	return nil
}

func (s *OrderService) lockOwned(ctx context.Context, tx domain.Repository, orderNo, botID string) (*domain.Order, error) {
	o, err := tx.FindByOrderNo(ctx, orderNo, true)
	if err != nil {
		return nil, mapRepoErr(err, orderNo)
	}
	if o.Status != domain.StatusInProgress || o.LockedBy == nil || *o.LockedBy != botID {
		return nil, apperror.LeaseLost(orderNo)
	}
	return o, nil
}

// Complete records the payment and marks the order COMPLETED.
// unique(payment_transactions.order_id) makes a second payment impossible.
func (s *OrderService) Complete(ctx context.Context, orderNo, botID, providerRef string) (*domain.Order, error) {
	if !domain.ValidOrderNo(orderNo) {
		return nil, apperror.InvalidOrderNo(orderNo)
	}
	if strings.TrimSpace(providerRef) == "" {
		return nil, apperror.Validation(map[string]string{"provider_ref": "required"})
	}
	var out *domain.Order
	err := s.repo.WithTx(ctx, func(tx domain.Repository) error {
		o, err := s.lockOwned(ctx, tx, orderNo, botID)
		if err != nil {
			return err
		}
		err = tx.CreatePayment(ctx, &domain.PaymentTransaction{
			OrderID: o.ID, BotID: botID, Amount: o.Amount, ProviderRef: providerRef, CreatedAt: s.Now(),
		})
		if errors.Is(err, domain.ErrDuplicate) {
			s.log.ErrorContext(ctx, "ALERT duplicate payment blocked", "orderNo", o.OrderNo, "botId", botID, "alert", true)
			return apperror.New(http.StatusConflict, "DUPLICATE_PAYMENT", "payment already recorded for this order", nil)
		}
		if err != nil {
			return err
		}
		out = o
		return s.transition(ctx, tx, o, domain.StatusCompleted, "TOPUP_SUCCEEDED",
			map[string]any{"bot_id": botID, "provider_ref": providerRef},
			map[string]any{"locked_by": nil, "locked_at": nil, "last_error": ""})
	})
	return out, err
}

// ReportFailure is called by a bot when the provider returns an error.
func (s *OrderService) ReportFailure(ctx context.Context, orderNo, botID, code, message string) (*domain.Order, error) {
	if !domain.ValidOrderNo(orderNo) {
		return nil, apperror.InvalidOrderNo(orderNo)
	}
	if strings.TrimSpace(code) == "" {
		return nil, apperror.Validation(map[string]string{"error_code": "required"})
	}
	var out *domain.Order
	err := s.repo.WithTx(ctx, func(tx domain.Repository) error {
		o, err := s.lockOwned(ctx, tx, orderNo, botID)
		if err != nil {
			return err
		}
		out = o
		return s.handleFailure(ctx, tx, o, code, message, map[string]any{"bot_id": botID})
	})
	return out, err
}

// handleFailure implements Transient vs Permanent error policy.
//   - Permanent  -> FAILED immediately (retrying a wrong UID is pointless).
//   - Transient  -> QUEUED with exponential backoff; after MaxRetries -> DELAYED + alert.
func (s *OrderService) handleFailure(ctx context.Context, tx domain.Repository, o *domain.Order,
	code, message string, meta map[string]any) error {

	if meta == nil {
		meta = map[string]any{}
	}
	meta["error_code"], meta["error_message"] = code, message
	lastErr := fmt.Sprintf("%s: %s", code, message)
	if len(lastErr) > 500 {
		lastErr = lastErr[:500]
	}
	clear := map[string]any{"locked_by": nil, "locked_at": nil, "last_error": lastErr}

	if domain.ClassifyErrorCode(code) == domain.ErrorPermanent {
		meta["error_kind"] = "PERMANENT"
		return s.transition(ctx, tx, o, domain.StatusFailed, "PERMANENT_ERROR: "+code, meta, clear)
	}

	meta["error_kind"] = "TRANSIENT"
	attempt := o.RetryCount + 1
	if attempt > o.MaxRetries {
		meta["retry_count"] = o.RetryCount
		if err := s.transition(ctx, tx, o, domain.StatusDelayed, "MAX_RETRIES_EXCEEDED: "+code, meta, clear); err != nil {
			return err
		}
		s.log.ErrorContext(ctx, "ALERT order moved to DELAYED after max retries",
			"orderNo", o.OrderNo, "retries", o.RetryCount, "errorCode", code, "alert", true)
		return nil
	}
	delay := Backoff(s.cfg.BackoffBase, s.cfg.BackoffMax, attempt)
	next := s.Now().Add(delay)
	meta["retry_count"], meta["next_attempt_at"], meta["backoff"] = attempt, next, delay.String()
	clear["retry_count"] = attempt
	clear["next_attempt_at"] = next
	return s.transition(ctx, tx, o, domain.StatusQueued, "TRANSIENT_ERROR_RETRY: "+code, meta, clear)
}
