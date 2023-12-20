package memcache

import (
	"context"
	"sync"
	"testing"

	"github.com/QuangTung97/go-memcache/memcache"
	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/cacheinv"
)

type clientTest struct {
	clients map[int64]*memcache.Client
	client  cacheinv.Client
}

var clientsOnce sync.Once
var globalClients map[int64]*memcache.Client

func initClients() map[int64]*memcache.Client {
	clientsOnce.Do(func() {
		client1, err := memcache.New("localhost:11211", 2)
		if err != nil {
			panic(err)
		}

		client2, err := memcache.New("localhost:11212", 2)
		if err != nil {
			panic(err)
		}

		globalClients = map[int64]*memcache.Client{
			11: client1,
			12: client2,
		}
	})
	return globalClients
}

func newClientTest(_ *testing.T) *clientTest {
	clients := initClients()
	for _, c := range clients {
		pipe := c.Pipeline()
		_ = pipe.FlushAll()()
		pipe.Finish()
	}

	return &clientTest{
		clients: clients,
		client:  NewClient(clients),
	}
}

func TestClient(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		c := newClientTest(t)

		assert.Equal(t, []int64{11, 12}, c.client.GetServerIDs())

		assert.Equal(t, "memcache:11", c.client.GetServerName(11))
		assert.Equal(t, "memcache:12", c.client.GetServerName(12))
	})

	t.Run("delete", func(t *testing.T) {
		c := newClientTest(t)

		client1 := c.clients[11]

		pipe := client1.Pipeline()

		setFn1 := pipe.MSet("key01", []byte("data01"), memcache.MSetOptions{})
		setFn2 := pipe.MSet("key02", []byte("data02"), memcache.MSetOptions{})
		setFn3 := pipe.MSet("key03", []byte("data03"), memcache.MSetOptions{})

		_, err := setFn1()
		assert.Equal(t, nil, err)
		_, err = setFn2()
		assert.Equal(t, nil, err)
		_, err = setFn3()
		assert.Equal(t, nil, err)

		// DO Delete
		err = c.client.DeleteCacheKeys(context.Background(), 11, []string{"key01", "key02"})
		assert.Equal(t, nil, err)

		// Get Cache Keys
		fn1 := pipe.MGet("key01", memcache.MGetOptions{})
		fn2 := pipe.MGet("key02", memcache.MGetOptions{})
		fn3 := pipe.MGet("key03", memcache.MGetOptions{})

		resp1, err := fn1()
		assert.Equal(t, nil, err)
		assert.Equal(t, "", string(resp1.Data))

		resp2, err := fn2()
		assert.Equal(t, nil, err)
		assert.Equal(t, "", string(resp2.Data))

		resp3, err := fn3()
		assert.Equal(t, nil, err)
		assert.Equal(t, "data03", string(resp3.Data))
	})

	t.Run("delete error", func(t *testing.T) {
		c := newClientTest(t)

		err := c.client.DeleteCacheKeys(context.Background(), 12, []string{"key01"})
		assert.Error(t, err)
	})
}
