package mysql

import (
	"context"
	"database/sql"
	"sort"

	"github.com/jmoiron/sqlx"

	"github.com/QuangTung97/cacheinv"
)

type repoImpl struct {
	db *sqlx.DB
}

var _ cacheinv.Repository = &repoImpl{}

func NewRepository(db *sqlx.DB) cacheinv.Repository {
	return &repoImpl{
		db: db,
	}
}

// GetLastEvents returns top *limit* events (events with the highest sequence numbers),
// by sequence number in ascending order, ignore events with null sequence number
func (r *repoImpl) GetLastEvents(ctx context.Context, limit uint64) ([]cacheinv.InvalidateEvent, error) {
	query := `
SELECT id, seq, data FROM invalidate_events
WHERE seq IS NOT NULL
ORDER BY seq DESC LIMIT ?
`
	var result []cacheinv.InvalidateEvent
	err := r.db.SelectContext(ctx, &result, query, limit)
	if err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Seq.Int64 < result[j].Seq.Int64
	})
	return result, nil
}

// GetUnprocessedEvents returns list of events with the smallest event *id* (not sequence number)
// *AND* have NULL sequence numbers, in ascending order of event *id*
// size of the list is limited by *limit*
func (r *repoImpl) GetUnprocessedEvents(ctx context.Context, limit uint64) ([]cacheinv.InvalidateEvent, error) {
	query := `
SELECT id, seq, data FROM invalidate_events
WHERE seq IS NULL
ORDER BY id LIMIT ?
`
	var result []cacheinv.InvalidateEvent
	err := r.db.SelectContext(ctx, &result, query, limit)
	return result, err
}

// GetEventsFrom returns list of events with sequence number >= *from*
// in ascending order of event sequence numbers, ignoring events with null sequence numbers
// size of the list is limited by *limit*
func (r *repoImpl) GetEventsFrom(ctx context.Context, from uint64, limit uint64) ([]cacheinv.InvalidateEvent, error) {
	query := `
SELECT id, seq, data FROM invalidate_events
WHERE seq >= ?
ORDER BY seq LIMIT ?
`
	var result []cacheinv.InvalidateEvent
	err := r.db.SelectContext(ctx, &result, query, from, limit)
	return result, err
}

// UpdateSequences updates only sequence numbers of *events*
func (r *repoImpl) UpdateSequences(ctx context.Context, events []cacheinv.InvalidateEvent) error {
	query := `
INSERT INTO invalidate_events (id, seq, data)
VALUES (:id, :seq, '')
ON DUPLICATE KEY UPDATE seq = IF(seq IS NULL, VALUES(seq), 'error')
`
	_, err := r.db.NamedExecContext(ctx, query, events)
	return err
}

// GetMinSequence returns the min sequence number of all events (except events with null sequence numbers)
// returns null if no events with sequence number existed
func (r *repoImpl) GetMinSequence(ctx context.Context) (sql.NullInt64, error) {
	return sql.NullInt64{}, nil
}

// DeleteEventsBefore deletes events with sequence number < *beforeSeq*
func (r *repoImpl) DeleteEventsBefore(ctx context.Context, beforeSeq uint64) error {
	return nil
}

// GetLastSequence get from invalidate_offsets table
func (r *repoImpl) GetLastSequence(ctx context.Context, serverName string) (sql.NullInt64, error) {
	return sql.NullInt64{}, nil
}

// SetLastSequence upsert into invalidate_offsets table
func (r *repoImpl) SetLastSequence(ctx context.Context, serverName string, seq int64) error {
	return nil
}
