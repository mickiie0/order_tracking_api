package usecase_test

// Integration tests against a real PostgreSQL (row locks, SKIP LOCKED and
// unique constraints cannot be faithfully mocked).
// Run:  TEST_DATABASE_URL=postgres://... go test ./...
// or:   docker compose --profile test run --rm test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inblack/order-tracking/internal/apperror"
	"github.com/inblack/order-tracking/internal/domain"
	"github.com/inblack/order-tracking/internal/provider"
	"github.com/inblack/order-tracking/internal/repository"
	"github.com/inblack/order-tracking/internal/usecase"
	"gorm.io/gorm"
)

var (
	dbOnce sync.Once
	testDB *gorm.DB
	dbErr  error
)

const secret = "test-secret"

func setup(t *testing.T) (*usecase.OrderService, *gorm.DB) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	dbOnce.Do(func() {
		testDB, dbErr = repository.Open(dsn)
		if dbErr == nil {
			dbErr = repository.Migrate(testDB)
		}
	})
	if dbErr != nil {
		t.Fatalf("db: %v", dbErr)
	}
	if err := testDB.Exec("TRUNCATE orders, order_timelines, payment_transactions, webhook_events RESTART IDENTITY").Error; err != nil {
		t.Fatal(err)
	}
	cfg := usecase.DefaultConfig()
	cfg.BackoffBase, cfg.BackoffMax = time.Millisecond, 5*time.Millisecond
	cfg.WebhookSecret = secret
	cfg.ReportDelayAfter = time.Minute
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return usecase.NewOrderService(repository.NewPostgres(testDB), provider.MockPaymentGateway{}, cfg, log), testDB
}

var keySeq atomic.Int64

func create(t *testing.T, svc *usecase.OrderService, uid string) *domain.Order {
	t.Helper()
	o, _, err := svc.CreateOrder(context.Background(), fmt.Sprintf("k-%d-%d", time.Now().UnixNano(), keySeq.Add(1)),
		usecase.CreateOrderInput{UserID: "u1", GameUID: uid, Amount: 100})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func webhook(svc *usecase.OrderService, eventID, orderNo string, amount float64) (*usecase.WebhookResult, error) {
	body, _ := json.Marshal(usecase.PaymentWebhook{EventID: eventID, OrderNo: orderNo,
		Type: usecase.WebhookPaymentSucceeded, Amount: amount, OccurredAt: time.Now()})
	return svc.HandlePaymentWebhook(context.Background(), body, usecase.Sign(secret, body))
}

func queued(t *testing.T, svc *usecase.OrderService, uid string) *domain.Order {
	t.Helper()
	o := create(t, svc, uid)
	if _, err := webhook(svc, "pay-"+o.OrderNo, o.OrderNo, o.Amount); err != nil {
		t.Fatal(err)
	}
	return o
}

func status(t *testing.T, svc *usecase.OrderService, no string) *domain.Order {
	t.Helper()
	o, err := svc.GetOrder(context.Background(), no)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func code(err error) string {
	var ae *apperror.Error
	if errors.As(err, &ae) {
		return ae.Code
	}
	return ""
}

// claimUntil keeps claiming (waiting out tiny backoff) until it gets the order.
func claimUntil(t *testing.T, svc *usecase.OrderService, bot string) *domain.Order {
	t.Helper()
	for i := 0; i < 200; i++ {
		o, err := svc.ClaimNext(context.Background(), bot)
		if err != nil {
			t.Fatal(err)
		}
		if o != nil {
			return o
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("nothing to claim")
	return nil
}

// ---------------------------------------------------------- Idempotency

func TestCreateOrder_IdempotencyKey_Concurrent(t *testing.T) {
	svc, db := setup(t)
	in := usecase.CreateOrderInput{UserID: "u1", GameUID: "123", Amount: 99}
	const n = 20
	ids := make(chan uint64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o, _, err := svc.CreateOrder(context.Background(), "same-key", in)
			if err != nil {
				t.Error(err)
				return
			}
			ids <- o.ID
		}()
	}
	wg.Wait()
	close(ids)
	first := uint64(0)
	for id := range ids {
		if first == 0 {
			first = id
		} else if id != first {
			t.Fatalf("different orders returned: %d vs %d", first, id)
		}
	}
	var count int64
	db.Model(&domain.Order{}).Count(&count)
	if count != 1 {
		t.Fatalf("expected 1 order, got %d", count)
	}
	// same key, different payload -> 422
	_, _, err := svc.CreateOrder(context.Background(), "same-key", usecase.CreateOrderInput{UserID: "u1", GameUID: "999", Amount: 99})
	if code(err) != "IDEMPOTENCY_KEY_REUSED" {
		t.Fatalf("want IDEMPOTENCY_KEY_REUSED, got %v", err)
	}
}

func TestGetOrder_Validation(t *testing.T) {
	svc, _ := setup(t)
	if _, err := svc.GetOrder(context.Background(), "BAD-123"); code(err) != "INVALID_ORDER_NO" {
		t.Fatalf("want INVALID_ORDER_NO got %v", err)
	}
	if _, err := svc.GetOrder(context.Background(), "ORD-20990101-1"); code(err) != "ORDER_NOT_FOUND" {
		t.Fatalf("want ORDER_NOT_FOUND got %v", err)
	}
}

// ---------------------------------------------------------- Problem 2: race / double payment

func TestClaim_ConcurrentBots_NoDoubleClaim(t *testing.T) {
	svc, _ := setup(t)
	const orders, bots = 30, 10
	for i := 0; i < orders; i++ {
		queued(t, svc, fmt.Sprintf("uid-%d", i))
	}
	var mu sync.Mutex
	claims := map[string]string{}
	var dup atomic.Int32
	var wg sync.WaitGroup
	for b := 0; b < bots; b++ {
		wg.Add(1)
		go func(bot string) {
			defer wg.Done()
			for {
				o, err := svc.ClaimNext(context.Background(), bot)
				if err != nil {
					t.Error(err)
					return
				}
				if o == nil {
					return
				}
				mu.Lock()
				if _, ok := claims[o.OrderNo]; ok {
					dup.Add(1)
				}
				claims[o.OrderNo] = bot
				mu.Unlock()
			}
		}(fmt.Sprintf("bot-%d", b))
	}
	wg.Wait()
	if dup.Load() != 0 {
		t.Fatalf("%d orders claimed more than once", dup.Load())
	}
	if len(claims) != orders {
		t.Fatalf("expected %d claims, got %d", orders, len(claims))
	}
}

func TestComplete_Concurrent_SinglePayment(t *testing.T) {
	svc, db := setup(t)
	o := queued(t, svc, "123")
	claimUntil(t, svc, "bot-A")

	var ok, conflict atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bot := "bot-A"
			if i%2 == 1 {
				bot = "bot-B" // a rogue bot that never owned the lease
			}
			_, err := svc.Complete(context.Background(), o.OrderNo, bot, fmt.Sprintf("ref-%d", i))
			if err == nil {
				ok.Add(1)
			} else if c := code(err); c == "LEASE_LOST" || c == "DUPLICATE_PAYMENT" {
				conflict.Add(1)
			} else {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if ok.Load() != 1 {
		t.Fatalf("expected exactly 1 successful completion, got %d", ok.Load())
	}
	var payments int64
	db.Model(&domain.PaymentTransaction{}).Where("order_id = ?", o.ID).Count(&payments)
	if payments != 1 {
		t.Fatalf("expected 1 payment, got %d", payments)
	}
	if s := status(t, svc, o.OrderNo).Status; s != domain.StatusCompleted {
		t.Fatalf("want COMPLETED got %s", s)
	}
}

func TestOptimisticLock_VersionConflict(t *testing.T) {
	svc, _ := setup(t)
	o := create(t, svc, "123")
	repo := repository.NewPostgres(testDB)
	a, _ := repo.FindByOrderNo(context.Background(), o.OrderNo, false)
	b, _ := repo.FindByOrderNo(context.Background(), o.OrderNo, false)
	if err := repo.UpdateCAS(context.Background(), a, map[string]any{"game_uid": "A"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateCAS(context.Background(), b, map[string]any{"game_uid": "B"}); !errors.Is(err, domain.ErrVersionConflict) {
		t.Fatalf("stale write must fail with ErrVersionConflict, got %v", err)
	}
	if a.Version != 2 {
		t.Fatalf("version should be 2, got %d", a.Version)
	}
}

// ---------------------------------------------------------- Problem 3: webhooks

func TestWebhook_DuplicateConcurrent_ProcessedOnce(t *testing.T) {
	svc, db := setup(t)
	o := create(t, svc, "123")
	var wg sync.WaitGroup
	var queuedN, dupN atomic.Int32
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := webhook(svc, "evt-1", o.OrderNo, o.Amount)
			if err != nil {
				t.Error(err)
				return
			}
			switch r.Result {
			case "QUEUED":
				queuedN.Add(1)
			case "DUPLICATE_IGNORED":
				dupN.Add(1)
			}
		}()
	}
	wg.Wait()
	if queuedN.Load() != 1 || dupN.Load() != 9 {
		t.Fatalf("queued=%d dup=%d", queuedN.Load(), dupN.Load())
	}
	var tl int64
	db.Model(&domain.OrderTimeline{}).Where("order_id = ? AND to_status = ? AND from_status = ?", o.ID, "QUEUED", "PENDING_PAYMENT").Count(&tl)
	if tl != 1 {
		t.Fatalf("expected 1 transition, got %d", tl)
	}
	// different event_id for same payment (bank re-sent) -> ignored by state validation
	r, err := webhook(svc, "evt-2", o.OrderNo, o.Amount)
	if err != nil || r.Result != "IGNORED_STATE_QUEUED" {
		t.Fatalf("got %+v %v", r, err)
	}
}

func TestWebhook_LateAfterExpiry_AutoRefund(t *testing.T) {
	svc, db := setup(t)
	o := create(t, svc, "123")
	db.Exec("UPDATE orders SET expires_at = now() - interval '1 minute' WHERE id = ?", o.ID)
	if n := svc.ExpirePending(context.Background()); n != 1 {
		t.Fatalf("expected 1 expired, got %d", n)
	}
	r, err := webhook(svc, "late-evt", o.OrderNo, o.Amount)
	if err != nil || r.Result != "AUTO_REFUNDED" {
		t.Fatalf("got %+v %v", r, err)
	}
	if s := status(t, svc, o.OrderNo).Status; s != domain.StatusRefunded {
		t.Fatalf("want REFUNDED got %s", s)
	}
}

func TestWebhook_InvalidSignature_And_AmountMismatch(t *testing.T) {
	svc, _ := setup(t)
	o := create(t, svc, "123")
	body := []byte(fmt.Sprintf(`{"event_id":"x","order_no":"%s","type":"payment.succeeded","amount":100}`, o.OrderNo))
	if _, err := svc.HandlePaymentWebhook(context.Background(), body, "deadbeef"); code(err) != "INVALID_SIGNATURE" {
		t.Fatalf("want INVALID_SIGNATURE got %v", err)
	}
	r, err := webhook(svc, "evt-amt", o.OrderNo, 1)
	if err != nil || r.Result != "AMOUNT_MISMATCH" {
		t.Fatalf("got %+v %v", r, err)
	}
	if s := status(t, svc, o.OrderNo).Status; s != domain.StatusPendingPayment {
		t.Fatalf("must remain PENDING_PAYMENT, got %s", s)
	}
}

// ---------------------------------------------------------- Problem 4: transient vs permanent

func TestTransientFailure_RetriesThenDelayed(t *testing.T) {
	svc, _ := setup(t)
	o := queued(t, svc, "DOWN-1")
	for attempt := 1; attempt <= 4; attempt++ {
		claimUntil(t, svc, "bot-1")
		got, err := svc.ReportFailure(context.Background(), o.OrderNo, "bot-1", "PROVIDER_UNAVAILABLE", "503")
		if err != nil {
			t.Fatal(err)
		}
		if attempt <= 3 {
			if got.Status != domain.StatusQueued || got.RetryCount != attempt || got.NextAttemptAt == nil {
				t.Fatalf("attempt %d: %+v", attempt, got)
			}
		} else if got.Status != domain.StatusDelayed {
			t.Fatalf("after max retries want DELAYED got %s", got.Status)
		}
	}
}

func TestPermanentFailure_FailedThenResolveWithNewUID(t *testing.T) {
	svc, _ := setup(t)
	o := queued(t, svc, "INVALID-1")
	claimUntil(t, svc, "bot-1")
	got, err := svc.ReportFailure(context.Background(), o.OrderNo, "bot-1", "INVALID_UID", "no such uid")
	if err != nil || got.Status != domain.StatusFailed || got.RetryCount != 0 {
		t.Fatalf("got %+v %v", got, err)
	}
	// wrong owner
	if _, err := svc.Resolve(context.Background(), o.OrderNo, "someone-else", usecase.ResolveInput{Action: "RETRY"}); code(err) != "FORBIDDEN" {
		t.Fatalf("want FORBIDDEN got %v", err)
	}
	r, err := svc.Resolve(context.Background(), o.OrderNo, "u1", usecase.ResolveInput{Action: "RETRY", GameUID: "777"})
	if err != nil || r.Status != domain.StatusQueued || r.GameUID != "777" {
		t.Fatalf("got %+v %v", r, err)
	}
	claimUntil(t, svc, "bot-2")
	if c, err := svc.Complete(context.Background(), o.OrderNo, "bot-2", "ref"); err != nil || c.Status != domain.StatusCompleted {
		t.Fatalf("got %+v %v", c, err)
	}
	// completed orders cannot be resolved
	if _, err := svc.Resolve(context.Background(), o.OrderNo, "u1", usecase.ResolveInput{Action: "REFUND"}); code(err) != "ORDER_NOT_RESOLVABLE" {
		t.Fatalf("want ORDER_NOT_RESOLVABLE got %v", err)
	}
}

// ---------------------------------------------------------- Problem 1: bot crash / lease

func TestLeaseReaper_ReclaimsStaleOrder(t *testing.T) {
	svc, db := setup(t)
	o := queued(t, svc, "123")
	claimUntil(t, svc, "bot-dead")
	db.Exec("UPDATE orders SET locked_at = now() - interval '1 hour' WHERE id = ?", o.ID)

	if n := svc.ReapStaleLeases(context.Background()); n != 1 {
		t.Fatalf("expected 1 reaped, got %d", n)
	}
	got := status(t, svc, o.OrderNo)
	if got.Status != domain.StatusQueued || got.RetryCount != 1 || got.LockedBy != nil {
		t.Fatalf("got %+v", got)
	}
	// the dead bot wakes up: heartbeat and complete must be rejected
	if err := svc.Heartbeat(context.Background(), o.OrderNo, "bot-dead"); code(err) != "LEASE_LOST" {
		t.Fatalf("want LEASE_LOST got %v", err)
	}
	if _, err := svc.Complete(context.Background(), o.OrderNo, "bot-dead", "ref"); code(err) != "LEASE_LOST" {
		t.Fatalf("want LEASE_LOST got %v", err)
	}
}

func TestHeartbeat_KeepsLeaseAlive(t *testing.T) {
	svc, _ := setup(t)
	o := queued(t, svc, "123")
	claimUntil(t, svc, "bot-1")
	if err := svc.Heartbeat(context.Background(), o.OrderNo, "bot-1"); err != nil {
		t.Fatal(err)
	}
	if n := svc.ReapStaleLeases(context.Background()); n != 0 {
		t.Fatalf("healthy lease must not be reaped, got %d", n)
	}
}

// ---------------------------------------------------------- Problem 5: expiry / SLA / report

func TestExpirePending_And_ListTabs(t *testing.T) {
	svc, db := setup(t)
	o1 := create(t, svc, "1")
	o2 := create(t, svc, "2")
	db.Exec("UPDATE orders SET expires_at = now() - interval '1 second' WHERE id = ?", o1.ID)
	if n := svc.ExpirePending(context.Background()); n != 1 {
		t.Fatalf("expected 1 expired, got %d", n)
	}
	cur, _ := svc.ListOrders(context.Background(), "current", "u1", 1, 10)
	hist, _ := svc.ListOrders(context.Background(), "history", "u1", 1, 10)
	if cur.Total != 1 || cur.Data[0].OrderNo != o2.OrderNo || hist.Total != 1 || hist.Data[0].OrderNo != o1.OrderNo {
		t.Fatalf("cur=%+v hist=%+v", cur, hist)
	}
	if _, err := svc.ListOrders(context.Background(), "bogus", "", 1, 10); code(err) != "VALIDATION_ERROR" {
		t.Fatalf("want VALIDATION_ERROR got %v", err)
	}
}

func TestReportIssue_EscalatesStuckQueued(t *testing.T) {
	svc, db := setup(t)
	fresh := queued(t, svc, "1")
	stuck := queued(t, svc, "2")
	db.Exec("UPDATE orders SET queued_at = now() - interval '1 hour' WHERE id = ?", stuck.ID)

	r, err := svc.ReportIssue(context.Background(), fresh.OrderNo, "u1", usecase.ReportIssueInput{Reason: "not received"})
	if err != nil || r.Escalated || r.Order.Status != domain.StatusQueued {
		t.Fatalf("fresh: %+v %v", r, err)
	}
	r, err = svc.ReportIssue(context.Background(), stuck.OrderNo, "u1", usecase.ReportIssueInput{Reason: "not received"})
	if err != nil || !r.Escalated || r.Order.Status != domain.StatusDelayed {
		t.Fatalf("stuck: %+v %v", r, err)
	}
	if n := svc.AlertStuckQueued(context.Background()); n != 0 {
		t.Fatalf("delayed order should no longer be in queue alerts, got %d", n)
	}
	tl, _ := svc.Timeline(context.Background(), stuck.OrderNo)
	if len(tl.Events) < 4 { // created, queued, report note, delayed
		t.Fatalf("timeline too short: %+v", tl.Events)
	}
}
