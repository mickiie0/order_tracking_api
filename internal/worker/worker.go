// Package worker contains background processes: the scheduler (cron-like
// maintenance) and simulated payment bots.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/inblack/order-tracking/internal/domain"
	"github.com/inblack/order-tracking/internal/usecase"
)

// Scheduler runs maintenance (expire QR, reap stale leases, SLA alerts) every Interval.
type Scheduler struct {
	Svc      *usecase.OrderService
	Interval time.Duration
	Log      *slog.Logger
}

func (s *Scheduler) Run(ctx context.Context) {
	t := time.NewTicker(s.Interval)
	defer t.Stop()
	s.Log.Info("scheduler started", "interval", s.Interval.String())
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r := s.Svc.RunMaintenance(ctx)
			if r.Expired+r.LeasesReaped+r.StuckQueued > 0 {
				s.Log.Info("maintenance tick", "expired", r.Expired, "leasesReaped", r.LeasesReaped, "stuckQueued", r.StuckQueued)
			}
		}
	}
}

// Bot simulates a payment bot: claim -> heartbeat while working -> complete/fail.
type Bot struct {
	ID           string
	Svc          *usecase.OrderService
	Provider     domain.GameProvider
	PollInterval time.Duration
	HeartbeatInt time.Duration
	Log          *slog.Logger
}

func (b *Bot) Run(ctx context.Context) {
	t := time.NewTicker(b.PollInterval)
	defer t.Stop()
	b.Log.Info("bot started", "botId", b.ID)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for b.processOne(ctx) { // drain while there is work
			}
		}
	}
}

func (b *Bot) processOne(ctx context.Context) bool {
	o, err := b.Svc.ClaimNext(ctx, b.ID)
	if err != nil {
		b.Log.Error("claim failed", "botId", b.ID, "err", err)
		return false
	}
	if o == nil {
		return false
	}
	log := b.Log.With("orderNo", o.OrderNo, "botId", b.ID)
	if strings.HasPrefix(strings.ToUpper(o.GameUID), "HANG") {
		log.Warn("SIMULATED BOT CRASH: abandoning order without heartbeat")
		return true
	}

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go b.heartbeat(workCtx, cancel, o.OrderNo, log)

	ref, err := b.Provider.TopUp(workCtx, o)
	if workCtx.Err() != nil && ctx.Err() == nil && err != nil {
		log.Warn("lease lost during top-up, dropping result")
		return true
	}
	if err != nil {
		code, msg := "UNKNOWN_PROVIDER_ERROR", err.Error()
		var pe *domain.ProviderError
		if errors.As(err, &pe) {
			code, msg = pe.Code, pe.Message
		}
		if _, ferr := b.Svc.ReportFailure(ctx, o.OrderNo, b.ID, code, msg); ferr != nil {
			log.Error("report failure failed", "err", ferr)
		}
		return true
	}
	if _, err := b.Svc.Complete(ctx, o.OrderNo, b.ID, ref); err != nil {
		log.Error("complete failed", "err", err)
	} else {
		log.Info("top-up completed", "providerRef", ref)
	}
	return true
}

func (b *Bot) heartbeat(ctx context.Context, cancel context.CancelFunc, orderNo string, log *slog.Logger) {
	t := time.NewTicker(b.HeartbeatInt)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := b.Svc.Heartbeat(ctx, orderNo, b.ID); err != nil {
				log.Warn("heartbeat rejected, aborting work", "err", err)
				cancel()
				return
			}
		}
	}
}
