package redis

import (
	"context"
	"fmt"
	"sort"

	"github.com/redis/go-redis/v9"

	"github.com/QuangTung97/cacheinv"
)

type clientImpl struct {
	serverIDs []int64
	clients   map[int64]*redis.Client
}

var _ cacheinv.Client = &clientImpl{}

// NewClient ...
func NewClient(clients map[int64]*redis.Client) cacheinv.Client {
	servers := make([]int64, 0, len(clients))
	for serverID := range clients {
		servers = append(servers, serverID)
	}
	sort.Slice(servers, func(i, j int) bool {
		return servers[i] < servers[j]
	})

	return &clientImpl{
		serverIDs: servers,
		clients:   clients,
	}
}

// GetServerIDs ...
func (c *clientImpl) GetServerIDs() []int64 {
	return c.serverIDs
}

// GetServerName ...
func (c *clientImpl) GetServerName(serverID int64) string {
	return fmt.Sprintf("redis:%d", serverID)
}

// DeleteCacheKeys ...
func (c *clientImpl) DeleteCacheKeys(ctx context.Context, serverID int64, keys []string) error {
	return c.clients[serverID].Del(ctx, keys...).Err()
}
