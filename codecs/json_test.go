package codecs_test

import (
	"github.com/kyuff/es"
	"github.com/kyuff/es/codecs"
	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/uuid"
	"math/rand/v2"
	"testing"
)

type EventMock struct {
	ID int `json:"id"`
}

func (EventMock) Name() string {
	return "EventMock"
}

func TestJSON(t *testing.T) {
	t.Run("should encode and decode", func(t *testing.T) {
		// arrange
		var (
			sut        = codecs.NewJSON()
			entityType = uuid.V7()
			in         = EventMock{ID: rand.Int()}
		)

		assert.NoError(t, sut.Register(entityType, EventMock{}))

		// act
		b, err := sut.Encode(es.Event{Content: in})

		// assert
		assert.NoError(t, err)

		// act
		got, err := sut.Decode(entityType, "EventMock", b)

		// assert
		assert.NoError(t, err)
		assert.Equal(t, in, got.(EventMock))
	})
}
