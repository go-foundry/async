# async

[![Go Reference](https://pkg.go.dev/badge/github.com/go-foundry/async.svg)](https://pkg.go.dev/github.com/go-foundry/async)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-foundry/async)](https://goreportcard.com/report/github.com/go-foundry/async)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Primitives for running concurrent tasks in Go with graceful shutdown,
parallel execution, and adaptive polling.

## Install

```bash
go get github.com/go-foundry/async
```

## Quick Start

### TaskRunner -- graceful shutdown

Run a long-lived process (HTTP server, gRPC, worker) with automatic
SIGINT/SIGTERM handling:

```go
runner := &async.TaskRunner{
    Start: func(ctx context.Context) error {
        return server.ListenAndServe()
    },
    Shutdown: func(ctx context.Context) error {
        return server.Shutdown(ctx)
    },
    ShutdownTimeout: 30 * time.Second,
}

if err := runner.Run(ctx); err != nil {
    log.Fatal(err)
}
```

### TaskGroup -- parallel execution

Run multiple tasks concurrently with optional concurrency limits:

```go
group := &async.TaskGroup{
    Tasks: []async.Task{httpServer, grpcServer, worker},
    MaxConcurrency: 0, // no limit
}

if err := group.Run(ctx); err != nil {
    log.Fatal(err)
}
```

### TaskScheduler -- adaptive polling

Schedule a task to run at regular intervals with multiple workers.
Workers independently adjust their polling frequency based on workload:

```go
scheduler := &async.TaskScheduler{
    Task: async.TaskFunc(func(ctx context.Context) error {
        n, err := processEvents(ctx)
        if err != nil {
            return err
        }
        switch {
        case n == 0:
            return &async.TimeoutError{Timeout: 30 * time.Second}
        case n < 100:
            return &async.TimeoutError{Timeout: 5 * time.Second}
        default:
            return &async.TimeoutError{Timeout: 500 * time.Millisecond}
        }
    }),
    MaxConcurrency: 10,
    Schedule:       500 * time.Millisecond,
}

if err := scheduler.Run(ctx); err != nil {
    log.Fatal(err)
}
```

## License

[MIT](LICENSE)
