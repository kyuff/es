package seqs

import (
	"iter"
)

func FilterErrors[T any](i iter.Seq2[T, error], x any) iter.Seq[T] {
	return func(yield func(T) bool) {
		for v, err := range i {
			if err != nil {
				continue
			}

			if !yield(v) {
				return
			}
		}
	}
}
