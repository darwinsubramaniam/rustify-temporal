// End-to-end check: the official Temporal Go SDK against rustify-temporal.
//
// Usage: go run . [host:port]   (the server must already be running; default 127.0.0.1:7233)
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const tq = "e2e-go"

func Greet(ctx context.Context, name string) (string, error) { return "Hello, " + name + "!", nil }

func Flaky(ctx context.Context) (int32, error) {
	info := activityInfo(ctx)
	if info < 3 {
		return 0, errors.New("try again")
	}
	return info, nil
}

func GreetingWorkflow(ctx workflow.Context, name string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
	var out string
	err := workflow.ExecuteActivity(ctx, Greet, name).Get(ctx, &out)
	return out, err
}

func RetryWorkflow(ctx workflow.Context) (int32, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, MaximumAttempts: 5},
	})
	var n int32
	err := workflow.ExecuteActivity(ctx, Flaky).Get(ctx, &n)
	return n, err
}

func CounterWorkflow(ctx workflow.Context) (int, error) {
	count := 0
	if err := workflow.SetQueryHandler(ctx, "count", func() (int, error) { return count, nil }); err != nil {
		return 0, err
	}
	ch := workflow.GetSignalChannel(ctx, "add")
	for count < 3 {
		var v int
		ch.Receive(ctx, &v)
		count += v
	}
	if err := workflow.Sleep(ctx, 500*time.Millisecond); err != nil {
		return 0, err
	}
	return count, nil
}

func ChildWorkflow(ctx workflow.Context, n int) (int, error) { return n + 1, nil }

func ParentWorkflow(ctx workflow.Context, n int) (int, error) {
	var out int
	err := workflow.ExecuteChildWorkflow(ctx, ChildWorkflow, n).Get(ctx, &out)
	return out, err
}

func check(name string, err error, ok bool, detail any) bool {
	if err != nil || !ok {
		fmt.Printf("FAIL  %-12s err=%v got=%v\n", name, err, detail)
		return false
	}
	fmt.Printf("PASS  %s\n", name)
	return true
}

func main() {
	addr := "127.0.0.1:7233"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	c, err := client.Dial(client.Options{HostPort: addr, Namespace: "default"})
	if err != nil {
		fmt.Println("connect:", err)
		os.Exit(1)
	}
	defer c.Close()
	w := worker.New(c, tq, worker.Options{})
	w.RegisterWorkflow(GreetingWorkflow)
	w.RegisterWorkflow(RetryWorkflow)
	w.RegisterWorkflow(CounterWorkflow)
	w.RegisterWorkflow(ChildWorkflow)
	w.RegisterWorkflow(ParentWorkflow)
	w.RegisterActivity(Greet)
	w.RegisterActivity(Flaky)
	if err := w.Start(); err != nil {
		fmt.Println("worker:", err)
		os.Exit(1)
	}
	defer w.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	id := func(p string) string { return fmt.Sprintf("%s-%d", p, time.Now().UnixNano()) }
	all := true

	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id("go-hello"), TaskQueue: tq}, GreetingWorkflow, "Go")
	var s string
	if err == nil {
		err = run.Get(ctx, &s)
	}
	all = check("hello", err, s == "Hello, Go!", s) && all

	run, err = c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id("go-retry"), TaskQueue: tq}, RetryWorkflow)
	var n int32
	if err == nil {
		err = run.Get(ctx, &n)
	}
	all = check("retry", err, n == 3, n) && all

	run, err = c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id("go-counter"), TaskQueue: tq}, CounterWorkflow)
	var total int
	if err == nil {
		for i := 0; i < 3 && err == nil; i++ {
			err = c.SignalWorkflow(ctx, run.GetID(), "", "add", 1)
		}
	}
	var q int
	if err == nil {
		for i := 0; i < 50; i++ {
			v, qerr := c.QueryWorkflow(ctx, run.GetID(), "", "count")
			err = qerr
			if err == nil {
				_ = v.Get(&q)
			}
			if q == 3 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	if err == nil {
		err = run.Get(ctx, &total)
	}
	all = check("signal+query", err, total == 3 && q == 3, []int{q, total}) && all

	run, err = c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id("go-parent"), TaskQueue: tq}, ParentWorkflow, 41)
	var r int
	if err == nil {
		err = run.Get(ctx, &r)
	}
	all = check("child", err, r == 42, r) && all

	if !all {
		os.Exit(1)
	}
	fmt.Println("all Go SDK checks passed")
}
