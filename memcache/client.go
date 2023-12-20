package memcache

import (
	"context"
	"fmt"
	"sort"

	"github.com/QuangTung97/go-memcache/memcache"

	"github.com/QuangTung97/cacheinv"
)

type clientImpl struct {
	serverIDs []int64
	clients   map[int64]*memcache.Client
}

var _ cacheinv.Client = &clientImpl{}

// NewClient ...
func NewClient(clients map[int64]*memcache.Client) cacheinv.Client {
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
	return fmt.Sprintf("memcache:%d", serverID)
}

// DeleteCacheKeys ...
func (c *clientImpl) DeleteCacheKeys(_ context.Context, serverID int64, keys []string) error {
	client := c.clients[serverID]

	pipe := client.Pipeline()
	defer pipe.Finish()

	fnList := make([]func() (memcache.MDelResponse, error), 0, len(keys))
	for _, key := range keys {
		fn := pipe.MDel(key, memcache.MDelOptions{})
		fnList = append(fnList, fn)
	}

	for _, fn := range fnList {
		_, err := fn()
		if err != nil {
			return err
		}
	}
	return nil
}
