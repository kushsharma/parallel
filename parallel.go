package parallel

import (
	"sync"
	"go.uber.org/ratelimit"
)

// Runner is a helper to execute operations in parallel
// and collect result/error in the end
type Runner struct {
	wg   sync.WaitGroup
	data []func() (interface{}, error)
	workers int
	ticketPerSec int
}

// State is provided as a result of each operation
type State struct {
	Val interface{}
	Err error
}

type indexedJob struct {
	index int
	fn func() (interface{}, error)
}

type indexedState struct {
	index int
	state State
}

// Add will queue provided function for execution and will not
// immediately execute it. The order in which add was called
// will be used to provide the (result, error) pair
func (p *Runner) Add(fn func() (interface{}, error)) {
	p.data = append(p.data, fn)
}

func (p *Runner) worker(inputCh chan indexedJob, outCh chan indexedState, rl ratelimit.Limiter) {
	defer p.wg.Done()
	for job := range inputCh {
		_ = rl.Take()
		res, err := job.fn()
		outCh <- indexedState{
			index: job.index,
			state: State{
				Val: res,
				Err: err,
			},
		}
	}
}

// Run is a blocking operation and wait for all jobs to finish.
// result will be an ordered array of State which maps
// one to one with the order in which routines are added via
// Add() fn
func (p *Runner) Run() []State {
	rl := ratelimit.New(p.ticketPerSec)
	inputCh := make(chan indexedJob)
	outCh := make(chan indexedState, len(p.data))

	// start workers
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(inputCh, outCh, rl)
	}

	// queue jobs
	for idx, fn := range p.data {
		inputCh <- indexedJob{
			index: idx,
			fn:    fn,
		}
	}
	close(inputCh)
	p.wg.Wait()

	// accumulate results
	states := make([]State, len(p.data))
	for idx := 0; idx < len(p.data); idx++ {
		st := <-outCh
		states[st.index] = st.state
	}
	close(outCh)
	return states
}

// RunSerial is here just to make sure runner can
// execute in serial as well if needed to
func (p *Runner) RunSerial() []State {
	states := make([]State, len(p.data))
	for runnerIndex, currentRoutine := range p.data {
		res, err := currentRoutine()
		states[runnerIndex] = State{
			Val: res,
			Err: err,
		}
	}
	return states
}

// NewRunner creates a new instance for parallel runner
func NewRunner(opts ... RunnerOption) *Runner {
	r := &Runner{
		wg:   sync.WaitGroup{},
		data: []func() (interface{}, error){},
		workers: 10000,
		ticketPerSec: 10000,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

type RunnerOption func(*Runner)

// WithLimit restricts n number of jobs executing in parallel
func WithLimit(l int) RunnerOption {
	return func(runner *Runner) {
		runner.workers = l
	}
}

// WithTicket restricts n number of jobs per second
func WithTicket(l int) RunnerOption {
	return func(runner *Runner) {
		runner.ticketPerSec = l
	}
}