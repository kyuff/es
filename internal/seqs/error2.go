package seqs

import "iter"

func Error2[T any](errs ...error) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for _, err := range errs {
			var item T
			if !yield(item, err) {
				return
			}
		}
	}
}
