package main

import (
	"os"
	"strconv"
	"time"

	"github.com/inblack/order-tracking/internal/usecase"
)

type appConfig struct {
	Port              string
	DatabaseURL       string
	SchedulerInterval time.Duration
	BotWorkers        int
	BotPollInterval   time.Duration
	BotHeartbeat      time.Duration
	ProviderLatency   time.Duration
	Seed              bool
	DevEndpoints      bool
	Usecase           usecase.Config
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(k)); err == nil {
		return d
	}
	return def
}

func envInt(k string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return n
	}
	return def
}

func loadConfig() appConfig {
	u := usecase.DefaultConfig()
	u.PaymentTTL = envDur("PAYMENT_TTL", u.PaymentTTL)
	u.LeaseTimeout = envDur("LEASE_TIMEOUT", u.LeaseTimeout)
	u.QueueSLA = envDur("QUEUE_SLA", u.QueueSLA)
	u.ReportDelayAfter = envDur("REPORT_DELAY_AFTER", u.ReportDelayAfter)
	u.BackoffBase = envDur("BACKOFF_BASE", u.BackoffBase)
	u.BackoffMax = envDur("BACKOFF_MAX", u.BackoffMax)
	u.MaxRetries = envInt("MAX_RETRIES", u.MaxRetries)
	u.WebhookSecret = env("WEBHOOK_SECRET", u.WebhookSecret)
	return appConfig{
		Port:              env("PORT", "8080"),
		DatabaseURL:       env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/orders?sslmode=disable"),
		SchedulerInterval: envDur("SCHEDULER_INTERVAL", 15*time.Second),
		BotWorkers:        envInt("BOT_WORKERS", 2),
		BotPollInterval:   envDur("BOT_POLL_INTERVAL", 3*time.Second),
		BotHeartbeat:      envDur("BOT_HEARTBEAT_INTERVAL", 30*time.Second),
		ProviderLatency:   envDur("PROVIDER_LATENCY", 500*time.Millisecond),
		Seed:              env("SEED_DATA", "true") == "true",
		DevEndpoints:      env("DEV_ENDPOINTS", "false") == "true",
		Usecase:           u,
	}
}
