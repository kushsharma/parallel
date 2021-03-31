# Parallel

Execute trivial operations in parallel without the hassle of maintaining channels,
wait groups, error accumulators and other boilerplate code.

[![Build Status](https://travis-ci.com/kushsharma/parallel.svg?branch=main)](https://travis-ci.com/kushsharma/parallel)

## How to use

```go
import (
    "github.com/kushsharma/parallel"
)

runner := parallel.NewRunner()
for _, j := range toomanyjobs {
	currentJob := j
	// queue operation for execution
	runner.Add(func() (interface{}, error) {
		//..do some operation here with (currentJob)..

		// first return value is result produced from the job
		// second return value is error if caused for some reason
		return nil, nil
	})
}

// Run() function is a blocking call and will start executing operations
// in parallel
for runIdx, state := range runner.Run() {
	if state.Err != nil {
		// handle error happened with job of index (runIdx)
	} else {
		// .. state.Val
		// result from from job with index (runIdx)
	}
}
```