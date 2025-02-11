package seqs

import "iter"

func EmptySeq2[K, V any]() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {}
}
