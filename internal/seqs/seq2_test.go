package seqs_test

import (
	"testing"

	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/seqs"
)

func TestSeq2(t *testing.T) {

	t.Run("return the full list", func(t *testing.T) {
		// arrange
		var (
			expected = []int{0, 1, 1, 2, 3, 5, 8, 13, 21, 34}
			got      []int
		)

		// act
		for n, err := range seqs.Seq2(expected...) {
			assert.NoError(t, err)
			got = append(got, n)
		}

		// assert
		assert.EqualSlice(t, expected, got)
	})

	t.Run("return empty list", func(t *testing.T) {
		// arrange
		var (
			expected = []int{}
			got      []int
		)

		// act
		for n, err := range seqs.Seq2(expected...) {
			assert.NoError(t, err)
			got = append(got, n)
		}

		// assert
		assert.EqualSlice(t, expected, got)
	})

}
