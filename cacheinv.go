package cacheinv

import (
	"context"
	"database/sql"
	"strings"
	"sync"

	"github.com/QuangTung97/eventx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =================================
// Common Types
// =================================

// InvalidateEvent ...
type InvalidateEvent struct {
	ID   int64         `db:"id"`
	Seq  sql.NullInt64 `db:"seq"`
	Data string        `db:"data"`
}

// GetID returns the event id
func (e InvalidateEvent) GetID() uint64 {
	return uint64(e.ID)
}

// GetSequence returns the event sequence number, = 0 if sequence is null
func (e InvalidateEvent) GetSequence() uint64 {
	return uint64(e.Seq.Int64)
}

// GetSize returns the approximate size (in bytes) of the event, for limit batch size by event data size
// using WithSubscriberSizeLimit for configuring this limit
func (e InvalidateEvent) GetSize() uint64 {
	return uint64(len(e.Data))
}

// Repository ...
type Repository interface {
	eventx.Repository[InvalidateEvent]

	// GetMinSequence returns the min sequence number of all events (except events with null sequence numbers)
	// returns null if no events with sequence number existed
	GetMinSequence(ctx context.Context) (sql.NullInt64, error)

	// DeleteEventsBefore deletes events with sequence number < *beforeSeq*
	DeleteEventsBefore(ctx context.Context, beforeSeq uint64) error

	// GetLastSequence get from invalidate_offsets table
	GetLastSequence(ctx context.Context, serverName string) (sql.NullInt64, error)
	// SetLastSequence upsert into invalidate_offsets table
	SetLastSequence(ctx context.Context, serverName string, seq int64) error
}

// Client ...
type Client interface {
	// GetServerIDs ...
	GetServerIDs() []int64

	// GetServerName ...
	GetServerName(serverID int64) string

	// DeleteCacheKeys ...
	DeleteCacheKeys(ctx context.Context, serverID int64, keys []string) error
}

// =================================
// Invalidator Job
// =================================

// InvalidatorJob ...
type InvalidatorJob struct {
	conf jobConfig

	ctx    context.Context
	cancel func()

	repo   Repository
	client Client

	runner    *eventx.Runner[InvalidateEvent]
	retention *eventx.RetentionJob[InvalidateEvent]
}

// NewInvalidatorJob ...
func NewInvalidatorJob(repo Repository, client Client, options ...Option) *InvalidatorJob {
	conf := newJobConfig(options)

	ctx, cancel := context.WithCancel(context.Background())

	j := &InvalidatorJob{
		conf: conf,

		ctx:    ctx,
		cancel: cancel,

		repo:   repo,
		client: client,
	}

	j.runner = eventx.NewRunner[InvalidateEvent](
		repo,
		func(event *InvalidateEvent, seq uint64) {
			event.Seq = sql.NullInt64{
				Valid: true,
				Int64: int64(seq),
			}
		},
		conf.runnerOptions...,
	)

	j.retention = eventx.NewRetentionJob[InvalidateEvent](
		j.runner,
		repo,
		conf.retentionOptions...,
	)

	return j
}

var cacheConsumerLastSeq = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cache_consumer_last_seq",
	Help: "last consumed sequence number for each cache server",
}, []string{"server_name"})

func (j *InvalidatorJob) runCacheRetryConsumer(serverID int64) {
	serverName := j.client.GetServerName(serverID)

	consumer := eventx.NewRetryConsumer[InvalidateEvent](
		j.runner,
		j.repo,
		func(ctx context.Context) (sql.NullInt64, error) {
			lastSeq, err := j.repo.GetLastSequence(j.ctx, serverName)
			if lastSeq.Valid {
				cacheConsumerLastSeq.WithLabelValues(serverName).Set(float64(lastSeq.Int64))
			}
			return lastSeq, err
		},
		func(ctx context.Context, seq uint64) error {
			err := j.repo.SetLastSequence(j.ctx, serverName, int64(seq))
			if err == nil {
				cacheConsumerLastSeq.WithLabelValues(serverName).Set(float64(seq))
			}
			return err
		},
		func(ctx context.Context, events []InvalidateEvent) error {
			var keys []string
			for _, e := range events {
				keys = append(keys, strings.Split(e.Data, ",")...)
			}
			return j.client.DeleteCacheKeys(j.ctx, serverID, keys)
		},
		j.conf.retryOptions...,
	)

	consumer.RunConsumer(j.ctx)
}

func (j *InvalidatorJob) runConsumers(wg *sync.WaitGroup) {
	servers := j.client.GetServerIDs()

	wg.Add(len(servers))

	for _, id := range servers {
		serverID := id

		go func() {
			defer wg.Done()
			j.runCacheRetryConsumer(serverID)
		}()
	}
}

// Run ...
func (j *InvalidatorJob) Run() {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		j.runner.Run(j.ctx)
	}()

	j.runConsumers(&wg)

	wg.Add(1)
	go func() {
		defer wg.Done()

		j.retention.RunJob(j.ctx)
	}()

	wg.Wait()
}

// Notify ...
func (j *InvalidatorJob) Notify() {
	j.runner.Signal()
}

// Shutdown ...
func (j *InvalidatorJob) Shutdown() {
	j.cancel()
}
