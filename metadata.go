package es

import (
	"context"
	"encoding/json"
)

type Metadata interface {
	Read(ctx context.Context) (json.RawMessage, error)
	Write(ctx context.Context, value json.RawMessage) (context.Context, error)
}
