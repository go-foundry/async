package async

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/alitto/pond/v2"
)

// Task represents a task that can be run by the worker.
type Task interface {
	// Run runs the task.
	Run(context.Context) error
}

var _ Task = &TaskRunner{}

// TaskRunner represents a server
type TaskRunner struct {
	// Start is the function to start the task.
	Start func(context.Context) error
	// Shutdown is the function to shut down the task.
	Shutdown func(context.Context) error
	// ShutdownTimeout is the timeout for shutting down the task.
	ShutdownTimeout time.Duration
}

// Start starts the operation and handles graceful shutdown.
func (x *TaskRunner) Run(pctx context.Context) error {
	nctx, nstop := signal.NotifyContext(pctx, syscall.SIGINT, syscall.SIGTERM)
	defer nstop()

	task := pond.SubmitErr(func() error {
		// wait for the context to be done
		<-nctx.Done()

		timeout := cmp.Or(x.ShutdownTimeout, time.Minute)
		ctx, stop := context.WithTimeout(pctx, timeout)
		defer stop()

		return x.Shutdown(ctx)
	})

	if err := x.Start(pctx); err != http.ErrServerClosed {
		return err
	}

	return task.Wait()
}

var _ Task = &TaskGroup{}

// TaskGroup allows running multiple runners in parallel.
type TaskGroup struct {
	// Tasks is a list of tasks to be run.
	Tasks []Task
	// MaxConcurrency represents the number of concurrent tasks.
	// It must be greater than or equal to 0 (0 means no limit).
	MaxConcurrency int
}

// Run starts all runners in parallel and waits for them to complete.
func (x *TaskGroup) Run(ctx context.Context) error {
	pool := pond.NewPool(x.MaxConcurrency)
	// group is a group of tasks that can be run in parallel
	group := pool.NewGroup()

	for _, runner := range x.Tasks {
		group.SubmitErr(func() error {
			return runner.Run(ctx)
		})
	}

	return group.Wait()
}

// TimeoutError represents a timeout error.
type TimeoutError struct {
	// Timeout is the timeout duration.
	Timeout time.Duration
}

// TimeoutError implements the error interface.
func (x *TimeoutError) Error() string {
	return fmt.Sprintf("timeout after %s", x.Timeout)
}

var _ Task = &TaskScheduler{}

// TaskScheduler is a worker that processes program coupons.
type TaskScheduler struct {
	// Task is the task to be run by the scheduler.
	Task Task
	// MaxConcurrency represents the number of concurrent tasks.
	// It must be greater than or equal to 0 (0 means no limit).
	MaxConcurrency int
	// Schedule is the interval between task executions.
	Schedule time.Duration
	// Cancel is used to cancel the worker's context and stop its operations.
	Cancel context.CancelCauseFunc
}

// Start starts the worker.
func (x *TaskScheduler) Run(ctx context.Context) error {
	// Using context.WithCancelCause to allow specifying a cancellation reason.
	ctx, x.Cancel = context.WithCancelCause(ctx)

	pool := pond.NewPool(x.MaxConcurrency)
	defer pool.Stop()

	group := pool.NewGroupContext(ctx)
	// default timeout can be controller by the taks
	timeout := int64(cmp.Or(x.Schedule, 500*time.Millisecond))
	// process the operation
	for {
		interval := time.Duration(atomic.LoadInt64(&timeout))
		select {
		case <-ctx.Done():
			return group.Wait()
		case <-time.After(interval):
			group.SubmitErr(func() error {
				err := x.Task.Run(ctx)

				var terr *TimeoutError
				// change the timeout
				if errors.As(err, &terr) {
					atomic.StoreInt64(&timeout, int64(terr.Timeout))
					return nil
				}

				return err
			})
		}
	}
}

// Shutdown shuts down the worker.
func (x *TaskScheduler) Shutdown(ctx context.Context) error {
	if x.Cancel != nil {
		x.Cancel(ctx.Err())
		x.Cancel = nil
	}
	return nil
}
