package seqs

import (
	"iter"
)

func Seq2[T any](items ...T) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for _, event := range items {
			if !yield(event, nil) {
				return
			}
		}
	}
}
