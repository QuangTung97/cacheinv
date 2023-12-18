package redis

import (
	"context"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/cacheinv"
)

type clientTest struct {
	redisClients map[int64]*redis.Client
	client       cacheinv.Client
}

var clientsOnce sync.Once
var globalClients map[int64]*redis.Client

func initClients() map[int64]*redis.Client {
	clientsOnce.Do(func() {
		globalClients = map[int64]*redis.Client{
			11: redis.NewClient(&redis.Options{
				Addr: "localhost:6379",
			}),
			12: redis.NewClient(&redis.Options{
				Addr: "localhost:6380",
			}),
		}
	})
	return globalClients
}

func newClientTest(_ *testing.T) *clientTest {
	clients := initClients()
	for _, c := range clients {
		err := c.FlushAll(context.Background()).Err()
		if err != nil {
			panic(err)
		}
	}

	return &clientTest{
		redisClients: clients,
		client:       NewClient(clients),
	}
}

func TestClient(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		c := newClientTest(t)
		assert.Equal(t, []int64{11, 12}, c.client.GetServerIDs())

		assert.Equal(t, "redis:11", c.client.GetServerName(11))
		assert.Equal(t, "redis:12", c.client.GetServerName(12))
	})

	ctx := context.Background()

	t.Run("delete key", func(t *testing.T) {
		c := newClientTest(t)

		client1 := c.redisClients[11]
		client2 := c.redisClients[12]

		err := client1.Set(ctx, "key01", []byte("data01"), 0).Err()
		assert.Equal(t, nil, err)

		err = client1.Set(ctx, "key02", []byte("data02"), 0).Err()
		assert.Equal(t, nil, err)

		err = client2.Set(ctx, "key03", []byte("data03"), 0).Err()
		assert.Equal(t, nil, err)

		err = c.client.DeleteCacheKeys(ctx, 11, []string{"key01", "key02"})
		assert.Equal(t, nil, err)

		val, err := client1.Get(ctx, "key01").Result()
		assert.Equal(t, redis.Nil, err)
		assert.Equal(t, "", val)

		val, err = client1.Get(ctx, "key02").Result()
		assert.Equal(t, redis.Nil, err)
		assert.Equal(t, "", val)

		val, err = client2.Get(ctx, "key03").Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, "data03", val)

		// Delete Redis 2 Not Success
		err = c.client.DeleteCacheKeys(ctx, 12, []string{"key02"})
		assert.Equal(t, nil, err)

		val, err = client2.Get(ctx, "key03").Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, "data03", val)

		// Delete Redis 2 Success
		err = c.client.DeleteCacheKeys(ctx, 12, []string{"key03"})
		assert.Equal(t, nil, err)

		val, err = client2.Get(ctx, "key03").Result()
		assert.Equal(t, redis.Nil, err)
		assert.Equal(t, "", val)
	})

	t.Run("delete partial", func(t *testing.T) {
		c := newClientTest(t)

		client1 := c.redisClients[11]

		err := client1.Set(ctx, "key01", []byte("data01"), 0).Err()
		assert.Equal(t, nil, err)

		err = client1.Set(ctx, "key02", []byte("data02"), 0).Err()
		assert.Equal(t, nil, err)

		err = c.client.DeleteCacheKeys(ctx, 11, []string{"key01"})
		assert.Equal(t, nil, err)

		val, err := client1.Get(ctx, "key01").Result()
		assert.Equal(t, redis.Nil, err)
		assert.Equal(t, "", val)

		val, err = client1.Get(ctx, "key02").Result()
		assert.Equal(t, nil, err)
		assert.Equal(t, "data02", val)
	})
}
