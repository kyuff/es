package seqs_test

import (
	"testing"

	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/seqs"
)

func TestError2(t *testing.T) {
	type testCase struct {
		name string
		errs []error
	}
	tests := []testCase{
		{
			name: "return the full list",
			errs: []error{nil, nil, nil},
		},
		{
			name: "return empty list",
			errs: []error{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []error

			for _, err := range seqs.Error2[int](tt.errs...) {
				got = append(got, err)
			}

			assert.EqualSlice(t, tt.errs, got)
		})
	}
}
