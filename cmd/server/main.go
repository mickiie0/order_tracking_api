package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/inblack/order-tracking/internal/handler"
	"github.com/inblack/order-tracking/internal/provider"
	"github.com/inblack/order-tracking/internal/repository"
	"github.com/inblack/order-tracking/internal/usecase"
	"github.com/inblack/order-tracking/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)
	cfg := loadConfig()

	db, err := repository.Open(cfg.DatabaseURL)
	if err != nil {
		log.Error("database connection failed", "err", err)
		os.Exit(1)
	}
	if err := repository.Migrate(db); err != nil {
		log.Error("migration failed", "err", err)
		os.Exit(1)
	}
	if cfg.Seed {
		if err := seed(db, log); err != nil {
			log.Error("seed failed", "err", err)
		}
	}

	repo := repository.NewPostgres(db)
	svc := usecase.NewOrderService(repo, provider.MockPaymentGateway{}, cfg.Usecase, log)
	gp := &provider.MockGameProvider{Latency: cfg.ProviderLatency}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	run := func(f func(context.Context)) { wg.Add(1); go func() { defer wg.Done(); f(ctx) }() }

	run((&worker.Scheduler{Svc: svc, Interval: cfg.SchedulerInterval, Log: log}).Run)
	for i := 1; i <= cfg.BotWorkers; i++ {
		b := &worker.Bot{ID: fmt.Sprintf("bot-%02d", i), Svc: svc, Provider: gp,
			PollInterval: cfg.BotPollInterval, HeartbeatInt: cfg.BotHeartbeat, Log: log}
		run(b.Run)
	}

	app := handler.NewApp(svc, log, cfg.DevEndpoints)
	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		_ = app.ShutdownWithTimeout(10 * time.Second)
	}()
	log.Info("server listening", "port", cfg.Port, "bots", cfg.BotWorkers)
	if err := app.Listen(":" + cfg.Port); err != nil {
		log.Error("server error", "err", err)
	}
	stop()
	wg.Wait()
}
