package seqs_test

import (
	"testing"

	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/seqs"
)

func TestEmptySeq2(t *testing.T) {
	// act
	got := seqs.EmptySeq2[string, error]()

	// assert
	assert.NotNil(t, got)
	for range got {
		t.Error("should be empty")
	}
}
