// Package async provides primitives for running concurrent tasks with graceful
// shutdown, parallel execution, and adaptive polling.
package async

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/alitto/pond/v2"
)

// Task represents a unit of work that can be executed by a runner, group,
// or scheduler.
type Task interface {
	// Run executes the task. Implementations should respect context
	// cancellation and return promptly when the context is done.
	Run(context.Context) error
}

// TaskFunc adapts an ordinary function to the [Task] interface.
type TaskFunc func(context.Context) error

// Run calls the underlying function.
func (f TaskFunc) Run(ctx context.Context) error {
	return f(ctx)
}

// notify relays the shutdown signals to the task runners. Tests replace it.
var notify = signal.Notify

var _ Task = &TaskRunner{}

// TaskRunner runs a long-lived operation with graceful shutdown on SIGINT
// and SIGTERM. It is typically used for HTTP servers, gRPC servers, or any
// blocking process that needs a coordinated start/shutdown lifecycle.
type TaskRunner struct {
	// Start launches the blocking operation. It receives a context that is
	// cancelled when shutdown begins: on SIGINT, at the end of ShutdownDelay
	// after SIGTERM, or when the parent context is done.
	// A nil Start is treated as a no-op.
	Start func(context.Context) error
	// Shutdown is called after Start returns or the context is cancelled.
	// It receives a context bounded by ShutdownTimeout. That context is not
	// cancelled when the parent context is done, so Shutdown can still wait
	// for in-flight work. A nil Shutdown is treated as a no-op.
	Shutdown func(context.Context) error
	// ShutdownDelay is how long the task keeps running after SIGTERM before
	// shutdown begins. Behind a service mesh or a load balancer, callers may
	// still route new requests to the process for a few seconds after
	// SIGTERM; an HTTP server that closes its listener at once refuses them.
	// A second signal skips the rest of the delay. SIGINT (Ctrl-C in a local
	// run), the parent context being done, or Start returning skip it too.
	// Zero means no delay.
	//
	// The delay only works when the runner receives SIGTERM itself. Do not
	// pass Run a context from [signal.NotifyContext] on SIGTERM: that context
	// is done at SIGTERM, which skips the delay.
	ShutdownDelay time.Duration
	// ShutdownTimeout bounds how long Shutdown may run. Defaults to 1 minute.
	ShutdownTimeout time.Duration
	// ErrorFilter, when set, is called for each non-nil error returned by
	// Start or Shutdown. If it returns true the error is discarded (filtered
	// out). This is useful for expected errors such as [http.ErrServerClosed].
	ErrorFilter func(error) bool
	// Logger logs the shutdown sequence: the signal, the delay, and the
	// start and end of Shutdown. Defaults to [slog.Default].
	Logger *slog.Logger
}

// Run starts the operation and handles graceful shutdown.
//
// It listens for SIGINT and SIGTERM to trigger shutdown. After SIGTERM it
// waits for ShutdownDelay first. When Start returns (for any reason),
// Shutdown is called with a timeout-bounded context.
// The returned error is the result of [errors.Join] on the Start and
// Shutdown errors, so callers can inspect both with [errors.Is].
func (x *TaskRunner) Run(pctx context.Context) error {
	signals := make(chan os.Signal, 2)
	notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	ctx, cancel := context.WithCancel(pctx)
	defer cancel()

	logger := cmp.Or(x.Logger, slog.Default())

	task := pond.SubmitErr(func() error {
		select {
		case sig := <-signals:
			logger.Info("The task has received a shutdown signal", slog.String("signal", sig.String()))
			x.delay(ctx, logger, sig, signals)
		case <-ctx.Done():
		}

		// Start's context is cancelled only now, after the delay
		cancel()

		if x.Shutdown == nil {
			return nil
		}

		timeout := cmp.Or(x.ShutdownTimeout, time.Minute)
		// The parent context may be done already; Shutdown still gets its timeout
		sctx, stop := context.WithTimeout(context.WithoutCancel(pctx), timeout)
		defer stop()

		logger.Info("The task is shutting down", slog.Duration("timeout", timeout))
		if err := x.Shutdown(sctx); err != nil {
			return err
		}
		logger.Info("The task has shut down")

		return nil
	})

	var startErr error
	if x.Start != nil {
		startErr = x.Start(ctx)
	}

	cancel()

	shutdownErr := task.Wait()

	if x.ErrorFilter != nil {
		if startErr != nil && x.ErrorFilter(startErr) {
			startErr = nil
		}
		if shutdownErr != nil && x.ErrorFilter(shutdownErr) {
			shutdownErr = nil
		}
	}

	return errors.Join(startErr, shutdownErr)
}

// delay blocks for ShutdownDelay after SIGTERM while the task keeps running.
// It returns early on a second signal or when the context is done.
func (x *TaskRunner) delay(ctx context.Context, logger *slog.Logger, sig os.Signal, signals <-chan os.Signal) {
	if sig != syscall.SIGTERM || x.ShutdownDelay <= 0 {
		return
	}

	logger.Info("The task keeps running for the shutdown delay", slog.Duration("delay", x.ShutdownDelay))

	timer := time.NewTimer(x.ShutdownDelay)
	defer timer.Stop()

	select {
	case <-timer.C:
		logger.Info("The shutdown delay is over", slog.Duration("delay", x.ShutdownDelay))
	case sig := <-signals:
		logger.Info("The task has received another signal, skipping the shutdown delay", slog.String("signal", sig.String()))
	case <-ctx.Done():
	}
}

var _ Task = &TaskGroup{}

// TaskGroup runs multiple tasks in parallel and waits for all of them
// to complete.
type TaskGroup struct {
	// Tasks is the set of tasks to execute.
	Tasks []Task
	// MaxConcurrency limits how many tasks run at once.
	// Zero means no limit.
	MaxConcurrency int
}

// Run starts all tasks in parallel and returns the first error, if any.
func (x *TaskGroup) Run(ctx context.Context) error {
	pool := pond.NewPool(x.MaxConcurrency)
	defer pool.Stop()

	group := pool.NewGroup()

	for _, runner := range x.Tasks {
		group.SubmitErr(func() error {
			return runner.Run(ctx)
		})
	}

	return group.Wait()
}

// TimeoutError represents a timeout error that tasks can return to dynamically
// adjust their polling interval. This enables adaptive polling where tasks can
// slow down when idle and speed up when busy.
//
// When a task returns a TimeoutError, the scheduler updates its interval for
// the next iteration and continues running (the error is not propagated).
type TimeoutError struct {
	// Timeout is the timeout duration to wait before the next task execution.
	Timeout time.Duration
}

// Error implements the error interface.
func (x *TimeoutError) Error() string {
	return fmt.Sprintf("timeout after %s", x.Timeout)
}

var _ Task = &TaskScheduler{}

// TaskScheduler schedules and runs tasks at regular intervals with support for
// concurrent workers and adaptive polling.
//
// Architecture:
//   - Spawns MaxConcurrency independent worker goroutines
//   - Each worker runs the same Task in a polling loop
//   - Workers share a cancellation context but maintain independent intervals
//   - Supports adaptive polling via TimeoutError
//
// Concurrency Model:
//   - MaxConcurrency workers poll simultaneously
//   - Database row locking (or similar mechanisms) prevent duplicate work
//   - Workers that find no work back off automatically
//   - Provides high availability and load distribution
//
// Adaptive Polling:
//   - Each worker independently adjusts its polling interval
//   - Tasks return TimeoutError to signal desired interval
//   - Workers finding work poll frequently; idle workers back off
//   - Example: 0 events → 30s, 1-99 events → 5s, 100-499 events → 2s, 500 events → 500ms
//
// Example Usage:
//
//	scheduler := &TaskScheduler{
//	    Task:           myTask,
//	    MaxConcurrency: 1000,
//	    Schedule:       500 * time.Millisecond,
//	}
//	if err := scheduler.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
type TaskScheduler struct {
	// Task is the task to be run by the scheduler.
	Task Task
	// MaxConcurrency represents the number of concurrent worker goroutines.
	// Each worker polls independently with its own adaptive interval.
	// If MaxConcurrency is 0, no workers are created.
	MaxConcurrency int
	// Schedule is the initial interval between task executions.
	// Defaults to 500ms if not specified.
	// Workers can dynamically adjust their interval by returning TimeoutError.
	Schedule time.Duration

	mu     sync.Mutex
	cancel context.CancelCauseFunc
}

// Run starts the task scheduler with MaxConcurrency concurrent workers.
//
// Each worker runs independently in its own goroutine, executing the Task in a
// polling loop with adaptive interval adjustment. All workers share the same
// cancellation context for coordinated shutdown.
//
// Behavior:
//   - Creates MaxConcurrency worker goroutines
//   - Each worker starts with the Schedule interval (default 500ms)
//   - Workers adapt their intervals independently via TimeoutError
//   - Returns when any worker returns a non-TimeoutError error
//   - All workers stop when context is cancelled or error occurs
//
// Error Handling:
//   - TimeoutError: Updates interval, continues running (not an error)
//   - Other errors: Stops scheduler and returns error
//   - Context cancellation: All workers exit cleanly
func (x *TaskScheduler) Run(ctx context.Context) error {
	var cctx context.Context

	x.mu.Lock()
	cctx, x.cancel = context.WithCancelCause(ctx)
	x.mu.Unlock()

	// Create pool with MaxConcurrency limit
	pool := pond.NewPool(x.MaxConcurrency)
	defer pool.Stop()

	// Create task group for waiting on all workers
	group := pool.NewGroup()

	// Submit MaxConcurrency workers
	for range x.MaxConcurrency {
		group.SubmitErr(func() error {
			return x.run(cctx)
		})
	}

	// Wait for all workers to complete or first error
	return group.Wait()
}

// run is the polling loop for a single worker goroutine.
func (x *TaskScheduler) run(ctx context.Context) error {
	interval := cmp.Or(x.Schedule, 500*time.Millisecond)

	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			err := x.Task.Run(ctx)

			var terr *TimeoutError
			if errors.As(err, &terr) {
				interval = terr.Timeout
			} else if err != nil {
				return err
			}

			timer.Reset(interval)
		}
	}
}

// Shutdown cancels the shared context, signaling all workers to stop.
// Workers exit their polling loops on the next iteration. Shutdown returns
// immediately without waiting for workers to finish.
// It is safe to call Shutdown before Run or multiple times.
func (x *TaskScheduler) Shutdown(ctx context.Context) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.cancel != nil {
		x.cancel(ctx.Err())
		x.cancel = nil
	}
	return nil
}
