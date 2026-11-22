// Benchmark: the same workloads, driven by the official Temporal Go SDK, against any Temporal
// server (rustify-temporal or the Go Temporal server). Prints one JSON object with the results.
//
// Usage: go run . -addr 127.0.0.1:7233 [-n 500] [-c 50]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const tq = "bench"

func Echo(ctx context.Context, s string) (string, error) { return s, nil }

// OneActivity: start, run one activity, complete.
func OneActivity(ctx workflow.Context, s string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
	var out string
	err := workflow.ExecuteActivity(ctx, Echo, s).Get(ctx, &out)
	return out, err
}

// FanOut: run n activities in parallel.
func FanOut(ctx workflow.Context, n int) (int, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
	futs := make([]workflow.Future, n)
	for i := range futs {
		futs[i] = workflow.ExecuteActivity(ctx, Echo, "x")
	}
	for _, f := range futs {
		if err := f.Get(ctx, nil); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// SignalQuery: wait for a signal, answer queries meanwhile.
func SignalQuery(ctx workflow.Context) (string, error) {
	got := ""
	_ = workflow.SetQueryHandler(ctx, "state", func() (string, error) { return got, nil })
	workflow.GetSignalChannel(ctx, "go").Receive(ctx, &got)
	return got, nil
}

type Stats struct {
	Scenario   string  `json:"scenario"`
	Workflows  int     `json:"workflows"`
	Concurrent int     `json:"concurrency"`
	Seconds    float64 `json:"seconds"`
	PerSecond  float64 `json:"workflows_per_second"`
	P50ms      float64 `json:"p50_ms"`
	P95ms      float64 `json:"p95_ms"`
	P99ms      float64 `json:"p99_ms"`
	Errors     int     `json:"errors"`
}

func run(name string, n, c int, one func(ctx context.Context, i int) error) Stats {
	lat := make([]time.Duration, 0, n)
	var mu sync.Mutex
	errs := 0
	jobs := make(chan int)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < c; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				t0 := time.Now()
				err := one(ctx, i)
				d := time.Since(t0)
				cancel()
				mu.Lock()
				if err != nil {
					errs++
					if errs <= 3 {
						fmt.Fprintf(os.Stderr, "%s error: %v\n", name, err)
					}
				} else {
					lat = append(lat, d)
				}
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	total := time.Since(start).Seconds()
	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	pct := func(p float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		return float64(lat[int(float64(len(lat)-1)*p)].Microseconds()) / 1000
	}
	return Stats{name, n, c, total, float64(n-errs) / total, pct(0.50), pct(0.95), pct(0.99), errs}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:7233", "server address")
	n := flag.Int("n", 500, "workflows per scenario")
	c := flag.Int("c", 50, "concurrent workflows")
	pollers := flag.Int("pollers", 16, "workflow and activity task pollers per worker")
	workers := flag.Int("workers", 4, "workers polling the task queue")
	flag.Parse()

	logger := sdklog.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	var cl client.Client
	var err error
	for i := 0; i < 100; i++ {
		cl, err = client.Dial(client.Options{HostPort: *addr, Namespace: "default", Logger: logger})
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	defer cl.Close()
	ensureNamespace(*addr, logger)
	// The Go SDK limits each worker to 1000 task polls per second, so one worker caps out near 500
	// workflows/s. Several workers on the same task queue keep the client from being the limit.
	for i := 0; i < *workers; i++ {
		w := worker.New(cl, tq, worker.Options{
			MaxConcurrentWorkflowTaskPollers: *pollers,
			MaxConcurrentActivityTaskPollers: *pollers,
		})
		w.RegisterWorkflow(OneActivity)
		w.RegisterWorkflow(FanOut)
		w.RegisterWorkflow(SignalQuery)
		w.RegisterActivity(Echo)
		if err := w.Start(); err != nil {
			fmt.Fprintln(os.Stderr, "worker:", err)
			os.Exit(1)
		}
		defer w.Stop()
	}

	opts := func(p string, i int) client.StartWorkflowOptions {
		return client.StartWorkflowOptions{ID: fmt.Sprintf("%s-%d-%d", p, time.Now().UnixNano(), i), TaskQueue: tq}
	}
	oneActivity := func(ctx context.Context, i int) error {
		r, err := cl.ExecuteWorkflow(ctx, opts("one", i), OneActivity, "hi")
		if err != nil {
			return err
		}
		return r.Get(ctx, nil)
	}
	// Warm up connections, caches and the database.
	run("warmup", 50, 10, oneActivity)

	results := []Stats{
		run("workflow with 1 activity", *n, *c, oneActivity),
		run("workflow with 10 parallel activities", *n/5, *c/5+1, func(ctx context.Context, i int) error {
			r, err := cl.ExecuteWorkflow(ctx, opts("fan", i), FanOut, 10)
			if err != nil {
				return err
			}
			return r.Get(ctx, nil)
		}),
		run("signal + query + result", *n/2, *c, func(ctx context.Context, i int) error {
			r, err := cl.ExecuteWorkflow(ctx, opts("sq", i), SignalQuery)
			if err != nil {
				return err
			}
			if _, err := cl.QueryWorkflow(ctx, r.GetID(), r.GetRunID(), "state"); err != nil {
				return err
			}
			if err := cl.SignalWorkflow(ctx, r.GetID(), r.GetRunID(), "go", "done"); err != nil {
				return err
			}
			return r.Get(ctx, nil)
		}),
	}
	_ = json.NewEncoder(os.Stdout).Encode(results)
}

// ensureNamespace registers "default" when the server doesn't have it yet.
func ensureNamespace(addr string, logger sdklog.Logger) {
	nc, err := client.NewNamespaceClient(client.Options{HostPort: addr, Logger: logger})
	if err != nil {
		return
	}
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := nc.Describe(ctx, "default"); err == nil {
		return
	}
	_ = nc.Register(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "default",
		WorkflowExecutionRetentionPeriod: durationpb.New(72 * time.Hour),
	})
	// Namespace caches refresh every few seconds on the Go server.
	for i := 0; i < 60; i++ {
		if _, err := nc.Describe(ctx, "default"); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	time.Sleep(12 * time.Second)
}
