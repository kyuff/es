package eventassert

import (
	"testing"

	"github.com/kyuff/es"
	"github.com/kyuff/es/internal/assert"
)

func EqualEvent(t *testing.T, expected, actual es.Event) bool {
	t.Helper()
	equal := []bool{
		assert.Equalf(t, expected.StreamID, actual.StreamID, "StreamID not equal"),
		assert.Equalf(t, expected.StreamType, actual.StreamType, "StreamType not equal"),
		assert.Equalf(t, expected.EventNumber, actual.EventNumber, "EventNumber not equal"),
		assert.Equalf(t, expected.Content, actual.Content, "Content not equal"),
	}
	for _, eq := range equal {
		if !eq {
			return false
		}
	}

	return true
}
