package parallel

import (
	"sync"
)

// Runner is a helper to execute operations in parallel
// and collect result/error in the end
type Runner struct {
	wg   sync.WaitGroup
	data []func() (interface{}, error)
}

// State is provided as a result of each operation
type State struct {
	Val interface{}
	Err error
}

// Add will queue provided function for execution and will not
// immediately execute it. The order in which add was called
// will be used to provide the (result, error) pair
func (p *Runner) Add(fn func() (interface{}, error)) {
	p.data = append(p.data, fn)
}

// Run is a blocking operation and wait for all jobs to finish.
// result will be an ordered array of State which maps
// one to one with the order in which routines are added via
// Add() fn
func (p *Runner) Run() []State {
	// modifying same array should be fine
	// as each index is unique for each runner
	states := make([]State, len(p.data))

	for idx, fn := range p.data {
		p.wg.Add(1)

		// Note(kush.sharma): don't refactor these scoped vars
		runnerIndex := idx
		currentRoutine := fn
		go func() {
			defer p.wg.Done()
			res, err := currentRoutine()
			states[runnerIndex] = State{
				Val: res,
				Err: err,
			}
		}()
	}
	p.wg.Wait()
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
func NewRunner() *Runner {
	return &Runner{
		wg:   sync.WaitGroup{},
		data: []func() (interface{}, error){},
	}
}
