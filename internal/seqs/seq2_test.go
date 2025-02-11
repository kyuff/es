package seqs_test

import (
	"testing"

	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/seqs"
)

func TestSeq2(t *testing.T) {
	type testCase struct {
		name  string
		items []int
	}
	tests := []testCase{
		{
			name:  "return the full list",
			items: []int{0, 1, 1, 2, 3, 5, 8, 13, 21, 34},
		},
		{
			name:  "return empty list",
			items: []int{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// arrange
			var got []int

			// act
			for n, err := range seqs.Seq2(tt.items...) {
				assert.NoError(t, err)
				got = append(got, n)
			}

			// assert
			assert.EqualSlice(t, tt.items, got)
		})
	}
}
