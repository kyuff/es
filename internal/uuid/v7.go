package uuid

import (
	"slices"
	"time"

	"github.com/gofrs/uuid/v5"
)

var gen uuid.Generator

func init() {
	gen = uuid.NewGen()
}

// V7 generates a random UUID
func V7() string {
	return uuid.Must(gen.NewV7()).String()
}

func V7AtTime(t time.Time) string {
	return uuid.Must(gen.NewV7AtTime(t)).String()
}

func V7At(t time.Time, count int) []string {
	var ids = make([]string, count)
	for i := range count {
		ids[i] = uuid.Must(gen.NewV7AtTime(t)).String()
	}

	slices.Sort(ids)
	return ids
}
