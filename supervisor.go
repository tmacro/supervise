package supervise

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
)

// Interface implemented by runnable tasks.
// It is expected that the tasks exits when the passed context closes.
type Runnable interface {
	Run(ctx context.Context) error
}

type RunnableFunc func(context.Context) error

func (rf RunnableFunc) Run(ctx context.Context) error {
	return rf(ctx)
}

// Tasks can implement this interface if they need to be restarted.
type Restartable interface {
	Restart(error, int) (bool, time.Duration)
}

type restartTask struct {
	Runnable
	maxRestarts int
}

func (rt *restartTask) Restart(_ error, i int) (bool, time.Duration) {
	if rt.maxRestarts == 0 {
		return true, 0
	}

	return i <= rt.maxRestarts, 0
}

var _ Restartable = (*restartTask)(nil)

func WithInfiniteRestart(t Runnable) Runnable {
	return restartTask{Runnable: t}
}

func WithNRestarts(t Runnable, maxRestarts int) Runnable {
	return restartTask{
		Runnable:    t,
		maxRestarts: maxRestarts,
	}
}

type backoffRestartTask struct {
	Runnable
	r    Restartable
	base time.Duration
}

func WithBackoff(t Runnable, base time.Duration) Runnable {
	task := backoffRestartTask{
		Runnable: t,
		base:     base,
	}

	r, ok := t.(Restartable)
	if ok {
		task.r = r
	}

	return task
}

func durPow(base time.Duration, exp int) time.Duration {
	var result time.Duration = 1
	for {
		if exp&1 == 1 {
			result *= base
		}
		exp >>= 1
		if exp == 0 {
			break
		}
		base *= base
	}

	return result
}

func (bac *backoffRestartTask) Restart(err error, i int) (restart bool, backoff time.Duration) {
	restart = true
	if bac.r != nil {
		restart, _ = bac.r.Restart(err, i)
	}

	if restart {
		return true, durPow(backoff, i)
	}

	return false, 0
}

var _ Restartable = (*backoffRestartTask)(nil)

var (
	ErrPanic error = errors.New("panic")
)

func run(ctx context.Context, task Runnable) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %w", ErrPanic, r.(error))
		}
	}()

	err = task.Run(ctx)
	return
}

const RestartResetInterval time.Duration = time.Second * 30

func runTask(ctx context.Context, task Runnable) error {
	var restarts int
	var lastRestart time.Time
	for {
		err := run(ctx, task)

		now := time.Now()
		if time.Since(lastRestart) > RestartResetInterval {
			restarts = 0
		}
		restarts += 1
		lastRestart = now

		var restart bool
		var backoff time.Duration
		r, ok := task.(Restartable)
		if ok {
			restart, backoff = r.Restart(err, restarts)
		}

		if !restart {
			return err
		}

		time.Sleep(backoff)
	}
}

type SimpleSupervisor struct {
	tasks []Runnable
}

var _ Runnable = (*SimpleSupervisor)(nil)

func NewSimpleSupervisor(tasks []Runnable) Runnable {
	return &SimpleSupervisor{tasks}
}

func (sup *SimpleSupervisor) Run(ctx context.Context) error {
	var eg *errgroup.Group
	eg, ctx = errgroup.WithContext(ctx)

	for _, task := range sup.tasks {
		eg.Go(func() error {
			return runTask(ctx, task)
		})
	}

	return eg.Wait()
}

type RestartStrategy int

const (
	RestartStrategyTemporary RestartStrategy = iota
	RestartStrategyOneShot
	RestartStrategyPermanent
)

func Run(ctx context.Context, tasks ...Runnable) error {
	sup := NewSimpleSupervisor(tasks)
	return sup.Run(ctx)
}
