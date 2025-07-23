package es

import (
	"context"
	"errors"
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
	t.Run("upgradeReader", func(t *testing.T) {
		t.Run("should upgrade events", func(t *testing.T) {
			// arrange
			var (
				streamType = newStreamType()
				events     = newEvents(5, streamType)
				rd         = readerFunc(func(ctx context.Context, streamType string, streamID string, eventNumber int64) iter.Seq2[Event, error] {
					return seqs.Seq2(events...)
				})
				calls    []int
				upgrades = []EventUpgrade{
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
				sut = newUpgradeReader(rd, upgrades)
			)

			// act
			_ = sut.Read(ctx, streamType, "", 0)

			// assert
			assert.EqualSlice(t, []int{1, 2, 3}, calls)
		})

		t.Run("should be able to stop stream", func(t *testing.T) {
			// arrange
			var (
				streamType = newStreamType()
				events     = newEvents(5, streamType)
				rd         = readerFunc(func(ctx context.Context, streamType string, streamID string, eventNumber int64) iter.Seq2[Event, error] {
					return seqs.Seq2(events...)
				})
				upgrades = []EventUpgrade{
					EventUpgradeFunc(func(ctx context.Context, i iter.Seq2[Event, error]) iter.Seq2[Event, error] {
						return i
					}),
				}
				sut = newUpgradeReader(rd, upgrades)
				got []Event
			)

			// act
			for event, _ := range sut.Read(ctx, streamType, "", 0) {
				got = append(got, event)
				if event.EventNumber > 2 {
					break
				}
			}

			// assert
			assert.EqualSlice(t, events[0:3], got)
		})

		t.Run("should stop stream on error", func(t *testing.T) {
			// arrange
			var (
				streamType = newStreamType()
				events     = newEvents(5, streamType)
				rd         = readerFunc(func(ctx context.Context, streamType string, streamID string, eventNumber int64) iter.Seq2[Event, error] {
					return seqs.Concat2(
						seqs.Seq2(events...),
						seqs.Error2[Event](errors.New("TEST")),
					)
				})
				upgrades = []EventUpgrade{
					EventUpgradeFunc(func(ctx context.Context, i iter.Seq2[Event, error]) iter.Seq2[Event, error] {
						return i
					}),
				}
				sut = newUpgradeReader(rd, upgrades)
				got []Event
			)

			// act
			for event, _ := range sut.Read(ctx, streamType, "", 0) {
				got = append(got, event)
			}

			// assert
			assert.EqualSlice(t, events, got)
		})
	})

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
