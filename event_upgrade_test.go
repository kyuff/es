package es

import (
	"context"
	"iter"
	"testing"

	"github.com/kyuff/es/internal/assert"
	"github.com/kyuff/es/internal/seqs"
	"github.com/kyuff/es/internal/uuid"
)

func Test_EventUpgrades(t *testing.T) {
	var (
		ctx           = context.Background()
		newStreamType = uuid.V7
		newStreamID   = uuid.V7
		newEvent      = func(id, typ string, mods ...func(e *Event)) Event {
			e := Event{
				StreamID:     id,
				StreamType:   typ,
				EventNumber:  1,
				StoreEventID: uuid.V7(),
			}
			for _, mod := range mods {
				mod(&e)
			}

			return e
		}
		newEvents = func(c int, typ string) []Event {
			var events []Event
			var streamID = newStreamID()
			for i := range c {
				events = append(events, newEvent(streamID, typ, func(e *Event) {
					e.EventNumber = int64(i) + 1
				}))
			}
			return events
		}
	)
	t.Run("upgradeWriter", func(t *testing.T) {
		t.Run("should upgrade events", func(t *testing.T) {
			// arrange
			var (
				streamType = newStreamType()
				events     = newEvents(3, streamType)
				seq        = seqs.Seq2(events...)
				calls      []int
				upgrades   = []EventUpgrade{
					EventUpgradeFunc(func(ctx context.Context, i iter.Seq2[Event, error]) iter.Seq2[Event, error] {
						calls = append(calls, 1)
						return i
					}),
					EventUpgradeFunc(func(ctx context.Context, i iter.Seq2[Event, error]) iter.Seq2[Event, error] {
						calls = append(calls, 2)
						return i
					}),
					EventUpgradeFunc(func(ctx context.Context, i iter.Seq2[Event, error]) iter.Seq2[Event, error] {
						calls = append(calls, 3)
						return i
					}),
				}
				w = writerFunc(func(ctx context.Context, streamType string, events iter.Seq2[Event, error]) error {
					calls = append(calls, 4)
					return nil
				})
				sut = newUpgradeWriter(w, map[string][]EventUpgrade{
					streamType: upgrades,
				})
			)

			// act
			err := sut.Write(ctx, streamType, seq)

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, []int{1, 2, 3, 4}, calls)
		})

		t.Run("should write with no upgrades", func(t *testing.T) {
			// arrange
			var (
				streamType = newStreamType()
				events     = newEvents(3, streamType)
				seq        = seqs.Seq2(events...)
				calls      []int
				upgrades   []EventUpgrade
				w          = writerFunc(func(ctx context.Context, streamType string, events iter.Seq2[Event, error]) error {
					calls = append(calls, 1)
					return nil
				})
				sut = newUpgradeWriter(w, map[string][]EventUpgrade{
					streamType: upgrades,
				})
			)

			// act
			err := sut.Write(ctx, streamType, seq)

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, []int{1}, calls)
		})

		t.Run("should write with no stream type", func(t *testing.T) {
			// arrange
			var (
				streamType = newStreamType()
				events     = newEvents(3, streamType)
				seq        = seqs.Seq2(events...)
				calls      []int
				upgrades   = []EventUpgrade{
					EventUpgradeFunc(func(ctx context.Context, i iter.Seq2[Event, error]) iter.Seq2[Event, error] {
						calls = append(calls, 2)
						return i
					}),
				}
				w = writerFunc(func(ctx context.Context, streamType string, events iter.Seq2[Event, error]) error {
					calls = append(calls, 1)
					return nil
				})
				sut = newUpgradeWriter(w, map[string][]EventUpgrade{
					"other": upgrades,
				})
			)

			// act
			err := sut.Write(ctx, streamType, seq)

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, []int{1}, calls)
		})
	})

	t.Run("combineEventUpgradesToWriter", func(t *testing.T) {
		t.Run("should combine event upgrades", func(t *testing.T) {
			// arrange
			var (
				streamType = newStreamType()
				events     = newEvents(3, streamType)
				seq        = seqs.Seq2(events...)
				calls      []int
				upgrades   = []EventUpgrade{
					EventUpgradeFunc(func(ctx context.Context, i iter.Seq2[Event, error]) iter.Seq2[Event, error] {
						calls = append(calls, 1)
						return i
					}),
					EventUpgradeFunc(func(ctx context.Context, i iter.Seq2[Event, error]) iter.Seq2[Event, error] {
						calls = append(calls, 2)
						return i
					}),
					EventUpgradeFunc(func(ctx context.Context, i iter.Seq2[Event, error]) iter.Seq2[Event, error] {
						calls = append(calls, 3)
						return i
					}),
				}
				w = writerFunc(func(ctx context.Context, streamType string, events iter.Seq2[Event, error]) error {
					calls = append(calls, 4)
					return nil
				})
				sut = combineEventUpgradesToWriter(w, upgrades)
			)

			// act
			err := sut.Write(ctx, streamType, seq)

			// assert
			assert.NoError(t, err)
			assert.EqualSlice(t, []int{1, 2, 3, 4}, calls)
		})
	})
}
