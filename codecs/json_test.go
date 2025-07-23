package codecs_test

import (
	"math/rand/v2"
	"testing"

	"github.com/kyuff/es"
	"github.com/kyuff/es/codecs"
	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/uuid"
)

type EventMock struct {
	ID int `json:"id"`
}

func (EventMock) EventName() string {
	return "EventMock"
}

func TestJSON(t *testing.T) {
	t.Run("return error on unknown type", func(t *testing.T) {
		// arrange
		var (
			sut = codecs.NewJSON()
		)

		// act
		_, err := sut.Decode("unknown", "unknown", []byte(`{}`))

		// assert
		assert.Error(t, err)
	})

	t.Run("return error on malformed json", func(t *testing.T) {
		// arrange
		var (
			sut        = codecs.NewJSON()
			streamType = uuid.V7()
		)

		// act
		_, err := sut.Decode(streamType, "EventMock", []byte(`{ ... not json`))

		// assert
		assert.Error(t, err)
	})

	t.Run("should encode and decode", func(t *testing.T) {
		// arrange
		var (
			sut        = codecs.NewJSON()
			streamType = uuid.V7()
			in         = EventMock{ID: rand.Int()}
		)

		assert.NoError(t, sut.Register(streamType, EventMock{}))

		// act
		b, err := sut.Encode(es.Event{Content: in})

		// assert
		assert.NoError(t, err)

		// act
		got, err := sut.Decode(streamType, "EventMock", b)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, in, got.(EventMock))
	})
}
