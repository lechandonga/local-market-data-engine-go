package market

import (
	"testing"
	"time"
)

var testBase = time.Date(2026, 10, 6, 9, 30, 0, 0, time.UTC)

func at(sec int) time.Time { return testBase.Add(time.Duration(sec) * time.Second) }

func mustNewEngine(t *testing.T, tardiness time.Duration, periods ...Period) *Engine {
	t.Helper()
	e, err := New(Config{
		DataDir:      t.TempDir(),
		Periods:      periods,
		MaxTardiness: tardiness,
		SyncWrites:   true,
		Now:          func() time.Time { return testBase },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func mustAccept(t *testing.T, e *Engine, tk Tick) {
	t.Helper()
	if r := e.Ingest(tk); !r.Accepted {
		t.Fatalf("expected accept for %s @ %s, got %s: %s", tk.Symbol, tk.Time, r.Reason, r.Detail)
	}
}

func expectReject(t *testing.T, e *Engine, tk Tick, want RejectReason) IngestResult {
	t.Helper()
	r := e.Ingest(tk)
	if r.Accepted {
		t.Fatalf("expected reject %s for %s @ %s, got accept", want, tk.Symbol, tk.Time)
	}
	if r.Reason != want {
		t.Fatalf("expected reason %s, got %s (%s)", want, r.Reason, r.Detail)
	}
	return r
}
