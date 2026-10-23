package task_test

import (
	"context"
	"testing"
	"time"
)

type settlementRetryStage struct {
	taskID     int64
	generation int64
	delay      time.Duration
	proceed    chan struct{}
}

type settlementRetryController struct {
	stages chan settlementRetryStage
}

func newSettlementRetryController() *settlementRetryController {
	return &settlementRetryController{stages: make(chan settlementRetryStage)}
}

func (c *settlementRetryController) wait(ctx context.Context, taskID, generation int64, delay time.Duration) error {
	stage := settlementRetryStage{taskID: taskID, generation: generation, delay: delay, proceed: make(chan struct{})}
	select {
	case c.stages <- stage:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-stage.proceed:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *settlementRetryController) next(t *testing.T, taskID, generation int64, delay time.Duration) func() {
	t.Helper()
	select {
	case stage := <-c.stages:
		if stage.taskID != taskID || stage.generation != generation || stage.delay != delay {
			t.Fatalf("settlement retry = task %d generation %d delay %s, want task %d generation %d delay %s", stage.taskID, stage.generation, stage.delay, taskID, generation, delay)
		}
		return func() { close(stage.proceed) }
	case <-t.Context().Done():
		t.Fatal("test canceled before settlement retry")
	case <-time.After(3 * handlerTestLeaseDuration):
		t.Fatal("task did not reach settlement retry")
	}
	return nil
}

func (c *settlementRetryController) allowRetries(t *testing.T, taskID, generation int64) {
	t.Helper()
	for _, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second} {
		c.next(t, taskID, generation, delay)()
	}
}
