package seqs_test

import (
	"iter"
	"testing"

	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/seqs"
)

func TestConcat2(t *testing.T) {
	type testCase struct {
		name string
		seqs []iter.Seq2[int, error]
		want []int
	}
	tests := []testCase{
		{
			name: "concat zero seqs",
			seqs: []iter.Seq2[int, error]{},
			want: []int{},
		},
		{
			name: "concat one seqs",
			seqs: []iter.Seq2[int, error]{
				seqs.Seq2(1, 2, 3),
			},
			want: []int{1, 2, 3},
		},
		{
			name: "concat two seqs",
			seqs: []iter.Seq2[int, error]{
				seqs.Seq2(1, 2, 3),
				seqs.Seq2(4, 5, 6),
			},
			want: []int{1, 2, 3, 4, 5, 6},
		},
		{
			name: "concat three seqs",
			seqs: []iter.Seq2[int, error]{
				seqs.Seq2(1, 2, 3),
				seqs.Seq2(4, 5, 6),
				seqs.Seq2(7, 8, 9),
			},
			want: []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// arrange
			var got []int

			// act
			for n, err := range seqs.Concat2(tt.seqs...) {
				assert.NoError(t, err)
				got = append(got, n)
			}

			// assert
			assert.EqualSlice(t, tt.want, got)
		})
	}

	t.Run("support breaking", func(t *testing.T) {
		// arrange
		var (
			sut = seqs.Concat2(seqs.Seq2(1, 2), seqs.Seq2(3, 4, 5))
			got = []int{}
		)

		// act
		for v, _ := range sut {
			got = append(got, v)
			if len(got) > 3 {
				break
			}
		}
		assert.EqualSlice(t, []int{1, 2, 3, 4}, got)
	})
}
