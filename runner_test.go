package async_test

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/go-foundry/async"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("TaskRunner", func() {
	It("runs Start and Shutdown in sequence", func() {
		var order []string

		runner := &async.TaskRunner{
			Start: func(ctx context.Context) error {
				order = append(order, "start")
				return nil
			},
			Shutdown: func(ctx context.Context) error {
				order = append(order, "shutdown")
				return nil
			},
		}

		err := runner.Run(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(order).To(Equal([]string{"start", "shutdown"}))
	})

	It("returns nil when Start is nil", func() {
		runner := &async.TaskRunner{
			Shutdown: func(ctx context.Context) error {
				return nil
			},
		}

		err := runner.Run(context.Background())
		Expect(err).NotTo(HaveOccurred())
	})

	It("handles nil Shutdown", func() {
		runner := &async.TaskRunner{
			Start: func(ctx context.Context) error {
				return nil
			},
		}

		err := runner.Run(context.Background())
		Expect(err).NotTo(HaveOccurred())
	})

	It("returns the Start error", func() {
		startErr := errors.New("start failed")

		runner := &async.TaskRunner{
			Start: func(ctx context.Context) error {
				return startErr
			},
			Shutdown: func(ctx context.Context) error {
				return nil
			},
		}

		err := runner.Run(context.Background())
		Expect(errors.Is(err, startErr)).To(BeTrue())
	})

	It("returns the Shutdown error", func() {
		shutdownErr := errors.New("shutdown failed")

		runner := &async.TaskRunner{
			Start: func(ctx context.Context) error {
				return nil
			},
			Shutdown: func(ctx context.Context) error {
				return shutdownErr
			},
		}

		err := runner.Run(context.Background())
		Expect(errors.Is(err, shutdownErr)).To(BeTrue())
	})

	It("joins Start and Shutdown errors", func() {
		startErr := errors.New("start failed")
		shutdownErr := errors.New("shutdown failed")

		runner := &async.TaskRunner{
			Start: func(ctx context.Context) error {
				return startErr
			},
			Shutdown: func(ctx context.Context) error {
				return shutdownErr
			},
		}

		err := runner.Run(context.Background())
		Expect(errors.Is(err, startErr)).To(BeTrue())
		Expect(errors.Is(err, shutdownErr)).To(BeTrue())
	})

	It("cancels Start when parent context is cancelled", func() {
		runner := &async.TaskRunner{
			Start: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			Shutdown: func(ctx context.Context) error {
				return nil
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		err := runner.Run(ctx)
		Expect(errors.Is(err, context.Canceled)).To(BeTrue())
	})

	It("uses ShutdownTimeout for the shutdown context", func() {
		runner := &async.TaskRunner{
			Start: func(ctx context.Context) error {
				return nil
			},
			Shutdown: func(ctx context.Context) error {
				deadline, ok := ctx.Deadline()
				Expect(ok).To(BeTrue())
				Expect(time.Until(deadline)).To(BeNumerically("<=", 100*time.Millisecond))
				Expect(time.Until(deadline)).To(BeNumerically(">", 0))
				return nil
			},
			ShutdownTimeout: 100 * time.Millisecond,
		}

		err := runner.Run(context.Background())
		Expect(err).NotTo(HaveOccurred())
	})
})

var _ = Describe("TaskGroup", func() {
	It("runs all tasks to completion", func() {
		var count atomic.Int32

		tasks := make([]async.Task, 5)
		for i := range tasks {
			tasks[i] = async.TaskFunc(func(ctx context.Context) error {
				count.Add(1)
				return nil
			})
		}

		group := &async.TaskGroup{Tasks: tasks}
		err := group.Run(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(count.Load()).To(Equal(int32(5)))
	})

	It("returns error from a failing task", func() {
		expectedErr := errors.New("task failed")

		group := &async.TaskGroup{
			Tasks: []async.Task{
				async.TaskFunc(func(ctx context.Context) error { return nil }),
				async.TaskFunc(func(ctx context.Context) error { return expectedErr }),
			},
		}

		err := group.Run(context.Background())
		Expect(err).To(MatchError(expectedErr))
	})

	It("runs with no tasks", func() {
		group := &async.TaskGroup{}
		err := group.Run(context.Background())
		Expect(err).NotTo(HaveOccurred())
	})

	It("limits concurrency to MaxConcurrency", func() {
		var running, peak atomic.Int32

		tasks := make([]async.Task, 10)
		for i := range tasks {
			tasks[i] = async.TaskFunc(func(ctx context.Context) error {
				cur := running.Add(1)
				defer running.Add(-1)

				for {
					old := peak.Load()
					if cur <= old || peak.CompareAndSwap(old, cur) {
						break
					}
				}

				time.Sleep(50 * time.Millisecond)
				return nil
			})
		}

		group := &async.TaskGroup{Tasks: tasks, MaxConcurrency: 2}
		err := group.Run(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(peak.Load()).To(BeNumerically("<=", 2))
	})
})

var _ = Describe("TimeoutError", func() {
	It("formats the error message", func() {
		err := &async.TimeoutError{Timeout: 5 * time.Second}
		Expect(err.Error()).To(Equal("timeout after 5s"))
	})

	It("can be detected with errors.As", func() {
		var target *async.TimeoutError
		err := &async.TimeoutError{Timeout: 2 * time.Second}
		Expect(errors.As(err, &target)).To(BeTrue())
		Expect(target.Timeout).To(Equal(2 * time.Second))
	})
})

var _ = Describe("TaskScheduler", func() {
	It("executes tasks at the scheduled interval", func() {
		var count atomic.Int32

		scheduler := &async.TaskScheduler{
			Task: async.TaskFunc(func(ctx context.Context) error {
				count.Add(1)
				return nil
			}),
			MaxConcurrency: 1,
			Schedule:       25 * time.Millisecond,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		err := scheduler.Run(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(count.Load()).To(BeNumerically(">=", 3))
	})

	It("adjusts interval via TimeoutError", func() {
		var count atomic.Int32

		scheduler := &async.TaskScheduler{
			Task: async.TaskFunc(func(ctx context.Context) error {
				n := count.Add(1)
				if n == 1 {
					return &async.TimeoutError{Timeout: 1 * time.Second}
				}
				return nil
			}),
			MaxConcurrency: 1,
			Schedule:       10 * time.Millisecond,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()

		err := scheduler.Run(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(count.Load()).To(Equal(int32(1)))
	})

	It("stops on non-timeout error", func() {
		expectedErr := errors.New("fatal")

		scheduler := &async.TaskScheduler{
			Task: async.TaskFunc(func(ctx context.Context) error {
				return expectedErr
			}),
			MaxConcurrency: 1,
			Schedule:       10 * time.Millisecond,
		}

		err := scheduler.Run(context.Background())
		Expect(err).To(MatchError(expectedErr))
	})

	It("stops via Shutdown", func() {
		scheduler := &async.TaskScheduler{
			Task: async.TaskFunc(func(ctx context.Context) error {
				return nil
			}),
			MaxConcurrency: 1,
			Schedule:       10 * time.Millisecond,
		}

		done := make(chan error, 1)
		go func() {
			done <- scheduler.Run(context.Background())
		}()

		time.Sleep(50 * time.Millisecond)
		Expect(scheduler.Shutdown(context.Background())).To(Succeed())
		Eventually(done).Should(Receive(BeNil()))
	})

	It("returns immediately when MaxConcurrency is zero", func() {
		scheduler := &async.TaskScheduler{
			Task: async.TaskFunc(func(ctx context.Context) error {
				Fail("task should not be called")
				return nil
			}),
			MaxConcurrency: 0,
		}

		err := scheduler.Run(context.Background())
		Expect(err).NotTo(HaveOccurred())
	})

	It("runs multiple workers concurrently", func() {
		var running, peak atomic.Int32

		scheduler := &async.TaskScheduler{
			Task: async.TaskFunc(func(ctx context.Context) error {
				cur := running.Add(1)
				defer running.Add(-1)

				for {
					old := peak.Load()
					if cur <= old || peak.CompareAndSwap(old, cur) {
						break
					}
				}

				time.Sleep(100 * time.Millisecond)
				return nil
			}),
			MaxConcurrency: 3,
			Schedule:       10 * time.Millisecond,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()

		err := scheduler.Run(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(peak.Load()).To(BeNumerically(">=", 2))
	})

	It("is safe to call Shutdown before Run", func() {
		scheduler := &async.TaskScheduler{}
		Expect(scheduler.Shutdown(context.Background())).To(Succeed())
	})
})
