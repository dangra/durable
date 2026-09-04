package main

import (
	"testing"
)

// TestStory runs the full demo and asserts its facts — not the print
// interleaving, which is concurrent across runs, but the durable
// outcomes each chapter claims.
func TestStory(t *testing.T) {
	w, trainResult, apiResult, err := story(t.Context(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	// Durability + at-least-once: the interrupted migration attempt was
	// re-executed by the next build and skipped idempotently.
	if w.skips != 1 {
		t.Errorf("idempotent migration re-executions = %d, want 1", w.skips)
	}

	// Evolution: the canary step, absent from the definition that
	// accepted the web deploy, executed for it after the restart.
	if w.canaried["web"] != 98 {
		t.Errorf("web canary score = %d, want 98 (step added mid-flight must execute)", w.canaried["web"])
	}
	if w.traffic["web"] != "v42" {
		t.Errorf("web traffic = %q, want v42", w.traffic["web"])
	}

	// Cancellation cascade: the train and its api child both terminate
	// as canceled, and the api deploy's unwind compensated both steps.
	if !trainResult.Canceled() {
		t.Errorf("train result = %+v, want canceled", trainResult)
	}
	if !apiResult.Canceled() {
		t.Errorf("api result = %+v, want canceled", apiResult.Result)
	}
	if len(w.rolledBack) != 1 || w.rolledBack[0] != "api" {
		t.Errorf("rolled back = %v, want [api]", w.rolledBack)
	}
	if len(w.tornDown) != 1 || w.tornDown[0] != "api" {
		t.Errorf("torn down = %v, want [api]", w.tornDown)
	}
	// The web deploy must be untouched by the freeze: its migrations
	// and traffic survive.
	if w.migrated["web"] != "v42" {
		t.Errorf("web migrations = %q, want v42 intact", w.migrated["web"])
	}
	if _, ok := w.traffic["api"]; ok {
		t.Error("api traffic was shifted; the freeze must land before shift-traffic")
	}
}
