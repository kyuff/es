package assert_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/seqs"
)

func TestAsserts(t *testing.T) {
	var testCases = []struct {
		name   string
		assert func(t *testing.T)
		failed bool
	}{
		{
			name: "EqualSeq2 success",
			assert: func(t *testing.T) {
				a := seqs.Seq2(1, 2)
				b := seqs.Seq2(1, 2)
				assert.EqualSeq2(t, a, b, func(expected, got assert.KeyValue[int, error]) bool {
					assert.Equal(t, expected.Key, got.Key)
					assert.Equal(t, expected.Value, got.Value)
					return true
				})
			},
			failed: false,
		},
		{
			name: "EqualSeq2 failed size",
			assert: func(t *testing.T) {
				a := seqs.Seq2(1, 2)
				b := seqs.Seq2(1, 2, 3)
				assert.EqualSeq2(t, a, b, func(expected, got assert.KeyValue[int, error]) bool {
					assert.Equal(t, expected.Key, got.Key)
					assert.Equal(t, expected.Value, got.Value)
					return true
				})
			},
			failed: true,
		},
		{
			name: "EqualSeq2 failed items",
			assert: func(t *testing.T) {
				a := seqs.Seq2(1, 2)
				b := seqs.Seq2(1, 3)
				assert.EqualSeq2(t, a, b, func(expected, got assert.KeyValue[int, error]) bool {
					return assert.Equal(t, expected.Key, got.Key)
				})
			},
			failed: true,
		},
		{
			name: "Equal success",
			assert: func(t *testing.T) {
				assert.Equal(t, 1, 1)
			},
			failed: false,
		},
		{
			name: "Equal failed",
			assert: func(t *testing.T) {
				assert.Equal(t, 1, 2)
			},
			failed: true,
		},
		{
			name: "Equalf success",
			assert: func(t *testing.T) {
				assert.Equalf(t, 1, 1, "hello test")
			},
			failed: false,
		},
		{
			name: "Equalf failed",
			assert: func(t *testing.T) {
				assert.Equalf(t, 1, 2, "hello test")
			},
			failed: true,
		},
		{
			name: "EqualSlice success",
			assert: func(t *testing.T) {
				assert.EqualSlice(t, []int{1, 2, 3}, []int{1, 2, 3})
			},
			failed: false,
		},
		{
			name: "EqualSlice failed size",
			assert: func(t *testing.T) {
				assert.EqualSlice(t, []int{1, 2, 3}, []int{})
			},
			failed: true,
		},
		{
			name: "EqualSlice failed item",
			assert: func(t *testing.T) {
				assert.EqualSlice(t, []int{1, 2, 3}, []int{1, 2, 4})
			},
			failed: true,
		},
		{
			name: "EqualSliceFunc success",
			assert: func(t *testing.T) {
				assert.EqualSliceFunc(t, []int{1, 2, 3}, []int{1, 2, 3}, func(want, item int) bool {
					return assert.Equal(t, want, item)
				})
			},
			failed: false,
		},
		{
			name: "EqualSliceFunc failed size",
			assert: func(t *testing.T) {
				assert.EqualSliceFunc(t, []int{1, 2, 3}, []int{1, 2}, func(want, item int) bool {
					return assert.Equal(t, want, item)
				})
			},
			failed: true,
		},
		{
			name: "EqualSliceFunc failed item",
			assert: func(t *testing.T) {
				assert.EqualSliceFunc(t, []int{1, 2, 3}, []int{1, 2, 4}, func(want, item int) bool {
					return assert.Equal(t, want, item)
				})
			},
			failed: true,
		},
		{
			name: "EqualTime success",
			assert: func(t *testing.T) {
				want := time.Now()
				got := want
				assert.EqualTime(t, want, got)
			},
			failed: false,
		},
		{
			name: "EqualTime failed",
			assert: func(t *testing.T) {
				want := time.Now()
				got := want.Add(time.Millisecond)
				assert.EqualTime(t, want, got)
			},
			failed: true,
		},
		{
			name: "NotEqual success",
			assert: func(t *testing.T) {
				assert.NotEqual(t, 1, 2)
			},
			failed: false,
		},
		{
			name: "NotEqual failed",
			assert: func(t *testing.T) {
				assert.NotEqual(t, 3, 3)
			},
			failed: true,
		},
		{
			name: "NotNil success",
			assert: func(t *testing.T) {
				var got = &strings.Builder{}
				assert.NotNil(t, got)
			},
			failed: false,
		},
		{
			name: "NotNil failed",
			assert: func(t *testing.T) {
				var got *strings.Builder
				assert.NotNil(t, got)
			},
			failed: true,
		},
		{
			name: "NoError success",
			assert: func(t *testing.T) {
				assert.NoError(t, nil)
			},
			failed: false,
		},
		{
			name: "NoError failed",
			assert: func(t *testing.T) {
				assert.NoError(t, errors.New("error"))
			},
			failed: true,
		},
		{
			name: "Error success",
			assert: func(t *testing.T) {
				assert.Error(t, errors.New("error"))
			},
			failed: false,
		},
		{
			name: "Error failed",
			assert: func(t *testing.T) {
				assert.Error(t, nil)
			},
			failed: true,
		},
		{
			name: "Truef success",
			assert: func(t *testing.T) {
				assert.Truef(t, true, "hello test")
			},
			failed: false,
		},
		{
			name: "Truef failed",
			assert: func(t *testing.T) {
				assert.Truef(t, false, "hello test")
			},
			failed: true,
		},
		{
			name: "NoError success",
			assert: func(t *testing.T) {
				assert.NoError(t, nil)
			},
			failed: false,
		},
		{
			name: "NoError failed",
			assert: func(t *testing.T) {
				assert.NoError(t, errors.New("error"))
			},
			failed: true,
		},
		{
			name: "Error success",
			assert: func(t *testing.T) {
				assert.Error(t, errors.New("error"))
			},
			failed: false,
		},
		{
			name: "Error failed",
			assert: func(t *testing.T) {
				assert.Error(t, nil)
			},
			failed: true,
		},
		{
			name: "Panic success",
			assert: func(t *testing.T) {
				assert.Panic(t, func() {
					panic("test")
				})
			},
			failed: false,
		},
		{
			name: "Panic failed",
			assert: func(t *testing.T) {
				assert.Panic(t, func() {
					// ... no panic
				})
			},
			failed: true,
		},
		{
			name: "NoPanic success",
			assert: func(t *testing.T) {
				assert.NoPanic(t, func() {
					// ... no panic
				})
			},
			failed: false,
		},
		{
			name: "NoPanic failed",
			assert: func(t *testing.T) {
				assert.NoPanic(t, func() {
					panic("test")
				})
			},
			failed: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// arrange
			var (
				x = &testing.T{}
			)

			// act
			testCase.assert(x)

			// arrange
			assert.Equal(t, testCase.failed, x.Failed())
		})
	}
}
