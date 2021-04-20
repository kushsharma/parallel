package parallel_test

import (
	"github.com/kushsharma/parallel"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/pkg/errors"
)

func TestRunner(t *testing.T) {
	t.Run("should run funcs in parallel with correct result and errors", func(t *testing.T) {
		tests := []struct {
			name      string
			funcs     []func() (interface{}, error)
			wantState []parallel.State
		}{
			{
				name: "with no result and no error",

				funcs: []func() (interface{}, error){
					func() (interface{}, error) {
						return nil, nil
					},
					func() (interface{}, error) {
						return nil, nil
					},
				},
				wantState: []parallel.State{{}, {}},
			},
			{
				name: "with no result and 2 error",
				funcs: []func() (interface{}, error){
					func() (interface{}, error) {
						return nil, errors.New("err - 1")
					},
					func() (interface{}, error) {
						return nil, nil
					},
					func() (interface{}, error) {
						return nil, errors.New("err - 2")
					},
				},
				wantState: []parallel.State{
					{
						Err: errors.New("err - 1"),
					},
					{},
					{
						Err: errors.New("err - 2"),
					},
				},
			},
			{
				name: "with 2 result and 0 error",
				funcs: []func() (interface{}, error){
					func() (interface{}, error) {
						return "result - 1", nil
					},
					func() (interface{}, error) {
						return nil, nil
					},
					func() (interface{}, error) {
						return "result - 2", nil
					},
				},
				wantState: []parallel.State{
					{
						Val: "result - 1",
					},
					{},
					{
						Val: "result - 2",
					},
				},
			},
			{
				name: "with 2 result and 1 error",
				funcs: []func() (interface{}, error){
					func() (interface{}, error) {
						return "result - 1", nil
					},
					func() (interface{}, error) {
						return nil, errors.New("err - 1")
					},
					func() (interface{}, error) {
						return "result - 2", nil
					},
				},
				wantState: []parallel.State{
					{
						Val: "result - 1",
					},
					{
						Err: errors.New("err - 1"),
					},
					{
						Val: "result - 2",
					},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				p := parallel.NewRunner(parallel.WithLimit(10))
				for _, fn := range tt.funcs {
					p.Add(fn)
				}
				states := p.Run()
				assert.Equal(t, len(states), len(tt.wantState))
				for i, e := range states {
					if tt.wantState[i].Err != e.Err {
						assert.Error(t, tt.wantState[i].Err, e.Err)
					}
					assert.Equal(t, tt.wantState[i].Val, e.Val)
				}
			})
		}
	})

	t.Run("should run funcs in parallel with concurrency worker limits", func(t *testing.T) {
		tests := []struct {
			name      string
			funcs     []func() (interface{}, error)
			wantState []parallel.State
		}{
			{
				name: "with 3 result and 1 error",
				funcs: []func() (interface{}, error){
					func() (interface{}, error) {
						time.Sleep(time.Millisecond * 300)
						return "result - 1", nil
					},
					func() (interface{}, error) {
						time.Sleep(time.Millisecond * 300)
						return nil, errors.New("err - 1")
					},
					func() (interface{}, error) {
						time.Sleep(time.Millisecond * 300)
						return "result - 2", nil
					},
					func() (interface{}, error) {
						time.Sleep(time.Millisecond * 300)
						return "result - 4", nil
					},
				},
				wantState: []parallel.State{
					{
						Val: "result - 1",
					},
					{
						Err: errors.New("err - 1"),
					},
					{
						Val: "result - 2",
					},
					{
						Val: "result - 4",
					},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				p := parallel.NewRunner(parallel.WithLimit(1))
				for _, fn := range tt.funcs {
					p.Add(fn)
				}
				start := time.Now()
				states := p.Run()
				elapsed := time.Since(start)
				assert.GreaterOrEqual(t, int(elapsed.Seconds()), 1)
				assert.Equal(t, len(states), len(tt.wantState))
				for i, e := range states {
					if tt.wantState[i].Err != e.Err {
						assert.Error(t, tt.wantState[i].Err, e.Err)
					}
					assert.Equal(t, tt.wantState[i].Val, e.Val)
				}
			})
		}
	})

	t.Run("should run funcs in parallel with concurrency time limits", func(t *testing.T) {
		tests := []struct {
			name      string
			funcs     []func() (interface{}, error)
			wantState []parallel.State
		}{
			{
				name: "no result no error",
				funcs: []func() (interface{}, error){
					func() (interface{}, error) {
						return nil, nil
					},
					func() (interface{}, error) {
						return nil, nil
					},
					func() (interface{}, error) {
						return nil, nil
					},
					func() (interface{}, error) {
						return nil, nil
					},
					func() (interface{}, error) {
						return nil, nil
					},
					func() (interface{}, error) {
						return nil, nil
					},
					func() (interface{}, error) {
						return nil, nil
					},
					func() (interface{}, error) {
						return nil, nil
					},
				},
				wantState: []parallel.State{
					{},	{},	{},	{}, {},	{},	{},	{},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				p := parallel.NewRunner(parallel.WithTicket(3))
				for _, fn := range tt.funcs {
					p.Add(fn)
				}
				start := time.Now()
				states := p.Run()
				elapsed := time.Since(start)
				assert.GreaterOrEqual(t, int(elapsed.Seconds()), 2)
				assert.Equal(t, len(states), len(tt.wantState))
			})
		}
	})


	t.Run("should run funcs in sequence with correct result and errors", func(t *testing.T) {
		tests := []struct {
			name      string
			funcs     []func() (interface{}, error)
			wantState []parallel.State
		}{
			{
				name: "with no result and no error",

				funcs: []func() (interface{}, error){
					func() (interface{}, error) {
						return nil, nil
					},
					func() (interface{}, error) {
						return nil, nil
					},
				},
				wantState: []parallel.State{{}, {}},
			},
			{
				name: "with no result and 2 error",
				funcs: []func() (interface{}, error){
					func() (interface{}, error) {
						return nil, errors.New("err - 1")
					},
					func() (interface{}, error) {
						return nil, nil
					},
					func() (interface{}, error) {
						return nil, errors.New("err - 2")
					},
				},
				wantState: []parallel.State{
					{
						Err: errors.New("err - 1"),
					},
					{},
					{
						Err: errors.New("err - 2"),
					},
				},
			},
			{
				name: "with 2 result and 0 error",
				funcs: []func() (interface{}, error){
					func() (interface{}, error) {
						return "result - 1", nil
					},
					func() (interface{}, error) {
						return nil, nil
					},
					func() (interface{}, error) {
						return "result - 2", nil
					},
				},
				wantState: []parallel.State{
					{
						Val: "result - 1",
					},
					{},
					{
						Val: "result - 2",
					},
				},
			},
			{
				name: "with 2 result and 1 error",
				funcs: []func() (interface{}, error){
					func() (interface{}, error) {
						return "result - 1", nil
					},
					func() (interface{}, error) {
						return nil, errors.New("err - 1")
					},
					func() (interface{}, error) {
						return "result - 2", nil
					},
				},
				wantState: []parallel.State{
					{
						Val: "result - 1",
					},
					{
						Err: errors.New("err - 1"),
					},
					{
						Val: "result - 2",
					},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				p := parallel.NewRunner()
				for _, fn := range tt.funcs {
					p.Add(fn)
				}
				states := p.RunSerial()
				assert.Equal(t, len(states), len(tt.wantState))
				for i, e := range states {
					if tt.wantState[i].Err != e.Err {
						assert.Error(t, tt.wantState[i].Err, e.Err)
					}
					assert.Equal(t, tt.wantState[i].Val, e.Val)
				}
			})
		}
	})
}
