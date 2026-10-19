package usecase

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	base, max := 30*time.Second, 5*time.Minute
	want := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 5 * time.Minute, 5 * time.Minute}
	for i, w := range want {
		if got := Backoff(base, max, i+1); got != w {
			t.Errorf("attempt %d: got %v want %v", i+1, got, w)
		}
	}
	if Backoff(base, max, 0) != base {
		t.Error("attempt<1 should be treated as 1")
	}
}

func TestWebhookSignature(t *testing.T) {
	body := []byte(`{"event_id":"e1"}`)
	sig := Sign("secret", body)
	if !VerifySignature("secret", body, sig) {
		t.Fatal("valid signature rejected")
	}
	if !VerifySignature("secret", body, "sha256="+sig) {
		t.Fatal("sha256= prefix should be accepted")
	}
	if VerifySignature("other", body, sig) {
		t.Fatal("wrong secret accepted")
	}
	if VerifySignature("secret", []byte(`{"event_id":"e2"}`), sig) {
		t.Fatal("tampered body accepted")
	}
	if VerifySignature("secret", body, "not-hex") || VerifySignature("secret", body, "") {
		t.Fatal("garbage signature accepted")
	}
}
