package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/jmoiron/sqlx"

	"github.com/QuangTung97/cacheinv"
)

type repoImpl struct {
	db              *sqlx.DB
	eventTableName  string
	offsetTableName string
}

var _ cacheinv.Repository = &repoImpl{}

// NewRepository ...
func NewRepository(
	db *sqlx.DB,
	eventTableName string,
	offsetTableName string,
) cacheinv.Repository {
	return &repoImpl{
		db:              db,
		eventTableName:  eventTableName,
		offsetTableName: offsetTableName,
	}
}

// GetLastEvents returns top *limit* events (events with the highest sequence numbers),
// by sequence number in ascending order, ignore events with null sequence number
func (r *repoImpl) GetLastEvents(ctx context.Context, limit uint64) ([]cacheinv.InvalidateEvent, error) {
	query := fmt.Sprintf(`
SELECT id, seq, data FROM %s
WHERE seq IS NOT NULL
ORDER BY seq DESC LIMIT ?
`, r.eventTableName)
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
	query := fmt.Sprintf(`
SELECT id, seq, data FROM %s
WHERE seq IS NULL
ORDER BY id LIMIT ?
`, r.eventTableName)
	var result []cacheinv.InvalidateEvent
	err := r.db.SelectContext(ctx, &result, query, limit)
	return result, err
}

// GetEventsFrom returns list of events with sequence number >= *from*
// in ascending order of event sequence numbers, ignoring events with null sequence numbers
// size of the list is limited by *limit*
func (r *repoImpl) GetEventsFrom(ctx context.Context, from uint64, limit uint64) ([]cacheinv.InvalidateEvent, error) {
	query := fmt.Sprintf(`
SELECT id, seq, data FROM %s
WHERE seq >= ?
ORDER BY seq LIMIT ?
`, r.eventTableName)
	var result []cacheinv.InvalidateEvent
	err := r.db.SelectContext(ctx, &result, query, from, limit)
	return result, err
}

// UpdateSequences updates only sequence numbers of *events*
func (r *repoImpl) UpdateSequences(ctx context.Context, events []cacheinv.InvalidateEvent) error {
	if len(events) == 0 {
		return nil
	}

	query := fmt.Sprintf(`
INSERT INTO %s (id, seq, data)
VALUES (:id, :seq, '')
ON DUPLICATE KEY UPDATE seq = IF(seq IS NULL, VALUES(seq), 'error')
`, r.eventTableName)
	_, err := r.db.NamedExecContext(ctx, query, events)
	return err
}

// GetMinSequence returns the min sequence number of all events (except events with null sequence numbers)
// returns null if no events with sequence number existed
func (r *repoImpl) GetMinSequence(ctx context.Context) (sql.NullInt64, error) {
	query := fmt.Sprintf(`SELECT MIN(seq) FROM %s`, r.eventTableName)
	var result sql.NullInt64
	err := r.db.GetContext(ctx, &result, query)
	return result, err
}

// DeleteEventsBefore deletes events with sequence number < *beforeSeq*
func (r *repoImpl) DeleteEventsBefore(ctx context.Context, beforeSeq uint64) error {
	query := fmt.Sprintf(`SELECT id FROM %s WHERE seq = ?`, r.eventTableName)
	var selectedID int64
	err := r.db.GetContext(ctx, &selectedID, query, beforeSeq)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}

	_, err = r.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROm %s WHERE id < ?`, r.eventTableName), selectedID)
	return err
}

// InvalidateOffset ...
type InvalidateOffset struct {
	ServerName string `db:"server_name"`
	LastSeq    int64  `db:"last_seq"`
}

// GetLastSequence get from invalidate_offsets table
func (r *repoImpl) GetLastSequence(ctx context.Context, serverName string) (sql.NullInt64, error) {
	query := fmt.Sprintf(`
SELECT server_name, last_seq FROM %s
WHERE server_name = ?
`, r.offsetTableName)
	var result InvalidateOffset
	err := r.db.GetContext(ctx, &result, query, serverName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return sql.NullInt64{}, nil
		}
		return sql.NullInt64{}, err
	}
	return sql.NullInt64{
		Valid: true,
		Int64: result.LastSeq,
	}, nil
}

// SetLastSequence upsert into invalidate_offsets table
func (r *repoImpl) SetLastSequence(ctx context.Context, serverName string, seq int64) error {
	query := fmt.Sprintf(`
INSERT INTO %s (server_name, last_seq)
VALUES (:server_name, :last_seq)
ON DUPLICATE KEY UPDATE last_seq = VALUES(last_seq)
`, r.offsetTableName)
	_, err := r.db.NamedExecContext(ctx, query, InvalidateOffset{
		ServerName: serverName,
		LastSeq:    seq,
	})
	return err
}
