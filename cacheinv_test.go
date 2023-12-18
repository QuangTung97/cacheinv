package cacheinv_test

import (
	"context"
	_ "embed"
	"sync"
	"testing"
	"time"

	"github.com/QuangTung97/eventx"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/cacheinv"
	"github.com/QuangTung97/cacheinv/mysql"
	redis_client "github.com/QuangTung97/cacheinv/redis"
)

type jobTest struct {
	db      *sqlx.DB
	repo    cacheinv.Repository
	clients map[int64]*redis.Client

	wg  sync.WaitGroup
	inv *cacheinv.InvalidatorJob
}

//go:embed mysql/migration.sql
var migrateSQL string

var dbOnce sync.Once
var globalDB *sqlx.DB

func initDB() *sqlx.DB {
	dbOnce.Do(func() {
		globalDB = sqlx.MustConnect(
			"mysql",
			"root:1@tcp(localhost:3306)/cache_inv?parseTime=true&multiStatements=true",
		)
		globalDB.MustExec(migrateSQL)
	})
	return globalDB
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

func newJobTest(_ *testing.T, options ...cacheinv.Option) *jobTest {
	db := initDB()

	db.MustExec(`TRUNCATE invalidate_events`)
	db.MustExec(`TRUNCATE invalidate_offsets`)

	repo := mysql.NewRepository(db, "invalidate_events", "invalidate_offsets")

	redisClients := initClients()
	for _, c := range redisClients {
		err := c.FlushAll(context.Background()).Err()
		if err != nil {
			panic(err)
		}
	}

	client := redis_client.NewClient(redisClients)

	return &jobTest{
		db:      db,
		repo:    repo,
		clients: redisClients,
		inv:     cacheinv.NewInvalidatorJob(repo, client, options...),
	}
}

func (j *jobTest) insertEvents(events ...cacheinv.InvalidateEvent) {
	query := `
INSERT INTO invalidate_events (data)
VALUES (:data)
`
	_, err := j.db.NamedExec(query, events)
	if err != nil {
		panic(err)
	}
}

func (j *jobTest) run() {
	j.wg.Add(1)
	go func() {
		defer j.wg.Done()
		j.inv.Run()
	}()
}

func (j *jobTest) waitCompleted() {
	j.inv.Shutdown()
	j.wg.Wait()
}

func TestInvalidatorJob(t *testing.T) {
	t.Run("do nothing", func(t *testing.T) {
		j := newJobTest(t)

		j.run()

		time.Sleep(500 * time.Millisecond)
		j.waitCompleted()

		lastSeq, err := j.repo.GetLastSequence(context.Background(), "redis:11")
		assert.Equal(t, nil, err)
		assert.Equal(t, true, lastSeq.Valid)
		assert.Equal(t, int64(0), lastSeq.Int64)
	})

	t.Run("do delete on redis", func(t *testing.T) {
		j := newJobTest(t)

		j.run()

		client1 := j.clients[11]
		client2 := j.clients[12]

		err := client1.Set(context.Background(), "key01", []byte("data01"), 0).Err()
		assert.Equal(t, nil, err)

		err = client1.Set(context.Background(), "key02", []byte("data02"), 0).Err()
		assert.Equal(t, nil, err)

		err = client2.Set(context.Background(), "key03", []byte("data03"), 0).Err()
		assert.Equal(t, nil, err)

		j.insertEvents(
			cacheinv.InvalidateEvent{
				Data: "key01,key02",
			},
			cacheinv.InvalidateEvent{
				Data: "key03",
			},
		)

		j.inv.Notify()

		time.Sleep(500 * time.Millisecond)
		j.waitCompleted()

		lastSeq, err := j.repo.GetLastSequence(context.Background(), "redis:11")
		assert.Equal(t, nil, err)
		assert.Equal(t, int64(2), lastSeq.Int64)

		lastSeq, err = j.repo.GetLastSequence(context.Background(), "redis:12")
		assert.Equal(t, nil, err)
		assert.Equal(t, int64(2), lastSeq.Int64)

		// Check redis
		val, err := client1.Get(context.Background(), "key01").Result()
		assert.Equal(t, redis.Nil, err)
		assert.Equal(t, "", val)

		val, err = client2.Get(context.Background(), "key03").Result()
		assert.Equal(t, redis.Nil, err)
		assert.Equal(t, "", val)
	})

	t.Run("do retention", func(t *testing.T) {
		j := newJobTest(t,
			cacheinv.WithRunnerOptions(eventx.WithCoreStoredEventsSize(512)),
			cacheinv.WithRetentionOptions(
				eventx.WithMaxTotalEvents(64),
				eventx.WithDeleteBatchSize(4),
			),
			cacheinv.WithRetryConsumerOptions(eventx.WithRetryConsumerFetchLimit(32)),
		)

		j.run()

		for i := 0; i < 128; i++ {
			j.insertEvents(
				cacheinv.InvalidateEvent{
					Data: "key01,key02",
				},
			)
		}

		j.inv.Notify()

		time.Sleep(2000 * time.Millisecond)

		j.waitCompleted()

		lastSeq, err := j.repo.GetLastSequence(context.Background(), "redis:11")
		assert.Equal(t, nil, err)
		assert.Equal(t, int64(128), lastSeq.Int64)

		minSeq, err := j.repo.GetMinSequence(context.Background())
		assert.Equal(t, nil, err)
		assert.Greater(t, minSeq.Int64, int64(128-64-5))

		lastSeq, err = j.repo.GetLastSequence(context.Background(), "redis:12")
		assert.Equal(t, nil, err)
		assert.Equal(t, int64(128), lastSeq.Int64)
	})
}
