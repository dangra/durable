package main

import (
	"fmt"
	"sync"

	"github.com/dangra/durable"
)

// world is the fake platform backend. It is shared across both engine
// generations — it stands for the real world (databases, load
// balancers), which is exactly what survives a daemon crash.
type world struct {
	mu sync.Mutex

	envs       map[string]bool   // service -> environment provisioned
	migrated   map[string]string // service -> schema version applied
	rolledBack []string          // services whose migrations were rolled back
	tornDown   []string          // services whose environments were torn down
	traffic    map[string]string // service -> live lb generation
	canaried   map[string]uint32 // service -> canary score
	skips      int               // idempotent migration re-executions

	webDeployID durable.RunID
	apiDeployID durable.RunID

	webMigrating     chan struct{} // closed when web's migration attempt is in flight
	apiCanaryRunning chan struct{} // closed when api's canary attempt is in flight
}

func newWorld() *world {
	return &world{
		envs:     map[string]bool{},
		migrated: map[string]string{},
		traffic:  map[string]string{},
		canaried: map[string]uint32{},

		webMigrating:     make(chan struct{}),
		apiCanaryRunning: make(chan struct{}),
	}
}

func (w *world) logf(format string, args ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	fmt.Printf(format+"\n", args...)
}

func (w *world) provision(service string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.envs[service] = true
	fmt.Printf("[%s] environment provisioned\n", service)
	return "env-" + service
}

func (w *world) teardown(service string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.envs, service)
	w.tornDown = append(w.tornDown, service)
	fmt.Printf("[%s] environment torn down (unwind)\n", service)
}

// migrate applies the schema unless it is already applied — the
// idempotency that makes at-least-once re-execution invisible.
func (w *world) migrate(service, version string) (applied bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.migrated[service] == version {
		w.skips++
		fmt.Printf("[%s] migrations already applied — idempotent re-execution\n", service)
		return false
	}
	w.migrated[service] = version
	fmt.Printf("[%s] migrations applied (%s)\n", service, version)
	return true
}

func (w *world) rollback(service string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.migrated, service)
	w.rolledBack = append(w.rolledBack, service)
	fmt.Printf("[%s] migrations rolled back (unwind)\n", service)
}
