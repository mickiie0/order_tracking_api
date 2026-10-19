package domain

import "testing"

func TestStateMachine_Transitions(t *testing.T) {
	cases := []struct {
		from, to OrderStatus
		ok       bool
	}{
		// normal flow
		{StatusPendingPayment, StatusQueued, true},
		{StatusQueued, StatusInProgress, true},
		{StatusInProgress, StatusCompleted, true},
		// failure flows
		{StatusPendingPayment, StatusExpired, true},
		{StatusInProgress, StatusQueued, true}, // transient retry / lease reaped
		{StatusInProgress, StatusFailed, true}, // permanent error
		{StatusInProgress, StatusDelayed, true},
		{StatusQueued, StatusDelayed, true},
		{StatusFailed, StatusQueued, true}, // resolve: fix UID
		{StatusFailed, StatusRefunded, true},
		{StatusDelayed, StatusRefunded, true},
		{StatusExpired, StatusRefunded, true}, // late webhook auto-refund
		// illegal
		{StatusPendingPayment, StatusCompleted, false},
		{StatusPendingPayment, StatusInProgress, false},
		{StatusExpired, StatusQueued, false}, // late webhook must NOT revive
		{StatusExpired, StatusCompleted, false},
		{StatusCompleted, StatusRefunded, false},
		{StatusCompleted, StatusQueued, false},
		{StatusRefunded, StatusQueued, false},
		{StatusQueued, StatusCompleted, false}, // must go through IN_PROGRESS
		{StatusFailed, StatusCompleted, false},
	}
	for _, c := range cases {
		if got := CanTransition(c.from, c.to); got != c.ok {
			t.Errorf("%s -> %s: got %v want %v", c.from, c.to, got, c.ok)
		}
	}
}

func TestStateMachine_TerminalAndKnown(t *testing.T) {
	for _, s := range []OrderStatus{StatusCompleted, StatusRefunded} {
		if !IsTerminal(s) {
			t.Errorf("%s should be terminal", s)
		}
	}
	all := append(append([]OrderStatus{}, CurrentStatuses...), HistoryStatuses...)
	if len(all) != 8 {
		t.Fatalf("tabs must cover all 8 statuses, got %d", len(all))
	}
	for _, s := range all {
		if !IsKnownStatus(s) {
			t.Errorf("%s missing from state machine", s)
		}
	}
	if IsKnownStatus("BOGUS") {
		t.Error("unknown status accepted")
	}
}

func TestValidOrderNo(t *testing.T) {
	valid := []string{"ORD-20261019-000001", "ORD-1-1"}
	invalid := []string{"", "ORD-20261019", "ord-20261019-1", "ORD-2026A019-1", "ORD-1-1; DROP TABLE", "XORD-1-1", "ORD--1"}
	for _, s := range valid {
		if !ValidOrderNo(s) {
			t.Errorf("%q should be valid", s)
		}
	}
	for _, s := range invalid {
		if ValidOrderNo(s) {
			t.Errorf("%q should be invalid", s)
		}
	}
}

func TestClassifyErrorCode(t *testing.T) {
	if ClassifyErrorCode("INVALID_UID") != ErrorPermanent || ClassifyErrorCode("UID_NOT_FOUND") != ErrorPermanent {
		t.Error("UID errors must be permanent")
	}
	for _, c := range []string{"PROVIDER_UNAVAILABLE", "PROVIDER_TIMEOUT", "BOT_LEASE_EXPIRED", "SOMETHING_NEW"} {
		if ClassifyErrorCode(c) != ErrorTransient {
			t.Errorf("%s must be transient", c)
		}
	}
}
