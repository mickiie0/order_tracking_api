package repository

import (
	"fmt"
	"time"

	"github.com/inblack/order-tracking/internal/domain"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Open connects with retries (DB container may still be booting).
func Open(dsn string) (*gorm.DB, error) {
	var lastErr error
	for i := 0; i < 30; i++ {
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
			TranslateError: true,
			Logger:         logger.Default.LogMode(logger.Warn),
			NowFunc:        func() time.Time { return time.Now().UTC() },
		})
		if err == nil {
			sqlDB, _ := db.DB()
			if err = sqlDB.Ping(); err == nil {
				sqlDB.SetMaxOpenConns(25)
				sqlDB.SetMaxIdleConns(10)
				return db, nil
			}
		}
		lastErr = err
		time.Sleep(time.Second)
	}
	return nil, fmt.Errorf("connect db: %w", lastErr)
}

// Migrate creates tables, sequence, and DB-level guards (CHECK constraints).
func Migrate(db *gorm.DB) error {
	if err := db.Exec(`CREATE SEQUENCE IF NOT EXISTS order_no_seq START 1`).Error; err != nil {
		return err
	}
	if err := db.AutoMigrate(&domain.Order{}, &domain.OrderTimeline{}, &domain.PaymentTransaction{}, &domain.WebhookEvent{}); err != nil {
		return err
	}
	stmts := []string{
		`DO $$ BEGIN
			ALTER TABLE orders ADD CONSTRAINT chk_orders_order_no CHECK (order_no ~ '^ORD-[0-9]+-[0-9]+$');
		EXCEPTION WHEN duplicate_object THEN NULL; END $$;`,
		`DO $$ BEGIN
			ALTER TABLE orders ADD CONSTRAINT chk_orders_status CHECK (status IN
			('PENDING_PAYMENT','QUEUED','IN_PROGRESS','COMPLETED','DELAYED','FAILED','EXPIRED','REFUNDED'));
		EXCEPTION WHEN duplicate_object THEN NULL; END $$;`,
		`DO $$ BEGIN
			ALTER TABLE orders ADD CONSTRAINT chk_orders_amount CHECK (amount > 0);
		EXCEPTION WHEN duplicate_object THEN NULL; END $$;`,
		// Partial index that exactly matches the claim query.
		`CREATE INDEX IF NOT EXISTS idx_orders_claimable ON orders (created_at, id)
			WHERE status = 'QUEUED'`,
	}
	for _, s := range stmts {
		if err := db.Exec(s).Error; err != nil {
			return err
		}
	}
	return nil
}
