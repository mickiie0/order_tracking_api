// Package provider contains mock adapters for the game top-up provider and
// the bank/payment gateway. Behaviour is driven by the Game UID prefix so the
// 5 problem scenarios can be reproduced deterministically:
//
//	INVALID*  -> permanent error INVALID_UID           (scenario 4)
//	DOWN*     -> transient error PROVIDER_UNAVAILABLE   (scenario 4 / 1)
//	FLAKY*    -> 50% transient PROVIDER_TIMEOUT         (scenario 4)
//	HANG*     -> bot runner simulates a crash           (scenario 1)
//	anything else -> success
package provider

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/inblack/order-tracking/internal/domain"
)

type MockGameProvider struct{ Latency time.Duration }

func (p *MockGameProvider) TopUp(ctx context.Context, o *domain.Order) (string, error) {
	select {
	case <-time.After(p.Latency):
	case <-ctx.Done():
		return "", &domain.ProviderError{Code: "PROVIDER_TIMEOUT", Message: ctx.Err().Error()}
	}
	uid := strings.ToUpper(o.GameUID)
	switch {
	case strings.HasPrefix(uid, "INVALID"):
		return "", &domain.ProviderError{Code: "INVALID_UID", Message: "game uid does not exist"}
	case strings.HasPrefix(uid, "DOWN"):
		return "", &domain.ProviderError{Code: "PROVIDER_UNAVAILABLE", Message: "provider returned 503"}
	case strings.HasPrefix(uid, "FLAKY") && rand.Intn(2) == 0:
		return "", &domain.ProviderError{Code: "PROVIDER_TIMEOUT", Message: "upstream timeout"}
	}
	return fmt.Sprintf("TOPUP-%s-%d", o.OrderNo, time.Now().UnixNano()), nil
}

type MockPaymentGateway struct{}

func (MockPaymentGateway) Refund(_ context.Context, orderNo string, _ float64) (string, error) {
	return fmt.Sprintf("REFUND-%s-%d", orderNo, time.Now().UnixNano()), nil
}
