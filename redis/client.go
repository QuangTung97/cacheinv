package redis

import (
	"context"

	"github.com/QuangTung97/cacheinv"
)

// Client ...
type Client interface {
	DeleteCacheKeys(ctx context.Context, events []cacheinv.InvalidateEvent) error
}

type clientImpl struct {
}
