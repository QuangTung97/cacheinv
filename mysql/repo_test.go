package mysql

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/QuangTung97/eventx/helpers"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/cacheinv"
)

type repoTest struct {
	ctx  context.Context
	db   *sqlx.DB
	repo cacheinv.Repository
}

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

//go:embed migration.sql
var migrateSQL string

func newRepoTest() *repoTest {
	db := initDB()

	db.MustExec(`TRUNCATE invalidate_events`)
	db.MustExec(`TRUNCATE invalidate_offsets`)

	return &repoTest{
		ctx:  context.Background(),
		db:   db,
		repo: NewRepository(db),
	}
}

func (r *repoTest) insertEvents(events ...cacheinv.InvalidateEvent) {
	query := `
INSERT INTO invalidate_events (data)
VALUES (:data)
`
	_, err := r.db.NamedExec(query, events)
	if err != nil {
		panic(err)
	}
}

func newInt64(v int64) sql.NullInt64 {
	return sql.NullInt64{
		Valid: true,
		Int64: v,
	}
}

func TestRepo_Eventx_Repo(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		r := newRepoTest()

		events, err := r.repo.GetLastEvents(r.ctx, 16)
		assert.Equal(t, nil, err)
		assert.Equal(t, 0, len(events))

		events, err = r.repo.GetUnprocessedEvents(r.ctx, 16)
		assert.Equal(t, nil, err)
		assert.Equal(t, 0, len(events))

		e1 := cacheinv.InvalidateEvent{
			Data: "key01,key02",
		}
		e2 := cacheinv.InvalidateEvent{
			Data: "key03,key04",
		}
		e3 := cacheinv.InvalidateEvent{
			Data: "key05",
		}

		r.insertEvents(e1, e2, e3)

		events, err = r.repo.GetLastEvents(r.ctx, 16)
		assert.Equal(t, nil, err)
		assert.Equal(t, 0, len(events))

		// Check Unprocessed Events
		e1.ID = 1
		e2.ID = 2
		e3.ID = 3

		events, err = r.repo.GetUnprocessedEvents(r.ctx, 16)
		assert.Equal(t, nil, err)
		assert.Equal(t, []cacheinv.InvalidateEvent{
			e1, e2, e3,
		}, events)

		// get with limit
		events, err = r.repo.GetUnprocessedEvents(r.ctx, 2)
		assert.Equal(t, nil, err)
		assert.Equal(t, []cacheinv.InvalidateEvent{
			e1, e2,
		}, events)

		minSeq, err := r.repo.GetMinSequence(r.ctx)
		assert.Equal(t, nil, err)
		assert.Equal(t, sql.NullInt64{}, minSeq)

		// Do update sequence
		e1.Seq = newInt64(11)
		e2.Seq = newInt64(12)

		err = r.repo.UpdateSequences(r.ctx, []cacheinv.InvalidateEvent{e1, e2})
		assert.Equal(t, nil, err)

		events, err = r.repo.GetLastEvents(r.ctx, 16)
		assert.Equal(t, nil, err)
		assert.Equal(t, []cacheinv.InvalidateEvent{
			e1, e2,
		}, events)

		events, err = r.repo.GetUnprocessedEvents(r.ctx, 16)
		assert.Equal(t, nil, err)
		assert.Equal(t, []cacheinv.InvalidateEvent{
			e3,
		}, events)

		// Do update sequence of the last event
		e3.Seq = newInt64(13)
		err = r.repo.UpdateSequences(r.ctx, []cacheinv.InvalidateEvent{e3})
		assert.Equal(t, nil, err)

		events, err = r.repo.GetLastEvents(r.ctx, 2)
		assert.Equal(t, nil, err)
		assert.Equal(t, []cacheinv.InvalidateEvent{
			e2, e3,
		}, events)

		// Get events from
		events, err = r.repo.GetEventsFrom(r.ctx, 11, 16)
		assert.Equal(t, nil, err)
		assert.Equal(t, []cacheinv.InvalidateEvent{
			e1, e2, e3,
		}, events)

		// Get events from with limit
		events, err = r.repo.GetEventsFrom(r.ctx, 11, 2)
		assert.Equal(t, nil, err)
		assert.Equal(t, []cacheinv.InvalidateEvent{
			e1, e2,
		}, events)

		// Get events from
		events, err = r.repo.GetEventsFrom(r.ctx, 12, 16)
		assert.Equal(t, nil, err)
		assert.Equal(t, []cacheinv.InvalidateEvent{
			e2, e3,
		}, events)

		minSeq, err = r.repo.GetMinSequence(r.ctx)
		assert.Equal(t, nil, err)
		assert.Equal(t, sql.NullInt64{
			Valid: true,
			Int64: 11,
		}, minSeq)
	})

	t.Run("from lib", func(t *testing.T) {
		r := newRepoTest()

		helpers.CheckRepoImpl[cacheinv.InvalidateEvent](
			t,
			r.repo,
			func(e *cacheinv.InvalidateEvent, id int64) {
				e.ID = id
			},
			func(e *cacheinv.InvalidateEvent, seq uint64) {
				e.Seq = sql.NullInt64{
					Valid: true,
					Int64: int64(seq),
				}
			},
			func() cacheinv.InvalidateEvent {
				index := rand.Intn(100_000)
				return cacheinv.InvalidateEvent{
					Data: fmt.Sprintf("KEY:%d", index),
				}
			},
			func(events []cacheinv.InvalidateEvent) {
				r.insertEvents(events...)
			},
			func() {
				r.db.MustExec(`TRUNCATE invalidate_events`)
			},
		)
	})

	t.Run("delete events", func(t *testing.T) {
		r := newRepoTest()

		e1 := cacheinv.InvalidateEvent{
			Data: "key01,key02",
		}
		e2 := cacheinv.InvalidateEvent{
			Data: "key03,key04",
		}
		e3 := cacheinv.InvalidateEvent{
			Data: "key05",
		}
		e4 := cacheinv.InvalidateEvent{
			Data: "key06",
		}

		minSeq, err := r.repo.GetMinSequence(r.ctx)
		assert.Equal(t, nil, err)
		assert.Equal(t, sql.NullInt64{}, minSeq)

		r.insertEvents(e1, e2, e3, e4)

		e1.ID = 1
		e2.ID = 2
		e3.ID = 3
		e4.ID = 4

		e1.Seq = newInt64(11)
		e2.Seq = newInt64(12)
		e3.Seq = newInt64(13)
		e4.Seq = newInt64(14)

		err = r.repo.UpdateSequences(r.ctx, []cacheinv.InvalidateEvent{e1, e2, e3, e4})
		assert.Equal(t, nil, err)

		minSeq, err = r.repo.GetMinSequence(r.ctx)
		assert.Equal(t, nil, err)
		assert.Equal(t, sql.NullInt64{
			Valid: true,
			Int64: 11,
		}, minSeq)

		// delete no events
		err = r.repo.DeleteEventsBefore(r.ctx, 10)
		assert.Equal(t, nil, err)

		minSeq, err = r.repo.GetMinSequence(r.ctx)
		assert.Equal(t, nil, err)
		assert.Equal(t, sql.NullInt64{
			Valid: true,
			Int64: 11,
		}, minSeq)

		// delete with events
		err = r.repo.DeleteEventsBefore(r.ctx, 13)
		assert.Equal(t, nil, err)

		minSeq, err = r.repo.GetMinSequence(r.ctx)
		assert.Equal(t, nil, err)
		assert.Equal(t, sql.NullInt64{
			Valid: true,
			Int64: 13,
		}, minSeq)
	})

	t.Run("empty", func(t *testing.T) {
		r := newRepoTest()

		err := r.repo.UpdateSequences(r.ctx, nil)
		assert.Equal(t, nil, err)
	})
}

func TestRepo_Repo_Offsets(t *testing.T) {
	const server1 = "SERVER01"
	const server2 = "SERVER02"

	t.Run("normal", func(t *testing.T) {
		r := newRepoTest()

		lastSeq, err := r.repo.GetLastSequence(r.ctx, server1)
		assert.Equal(t, nil, err)
		assert.Equal(t, sql.NullInt64{}, lastSeq)

		err = r.repo.SetLastSequence(r.ctx, server1, 11)
		assert.Equal(t, nil, err)

		lastSeq, err = r.repo.GetLastSequence(r.ctx, server1)
		assert.Equal(t, nil, err)
		assert.Equal(t, sql.NullInt64{
			Valid: true,
			Int64: 11,
		}, lastSeq)

		// server 2
		lastSeq, err = r.repo.GetLastSequence(r.ctx, server2)
		assert.Equal(t, nil, err)
		assert.Equal(t, sql.NullInt64{}, lastSeq)

		err = r.repo.SetLastSequence(r.ctx, server2, 21)
		assert.Equal(t, nil, err)

		lastSeq, err = r.repo.GetLastSequence(r.ctx, server2)
		assert.Equal(t, nil, err)
		assert.Equal(t, sql.NullInt64{
			Valid: true,
			Int64: 21,
		}, lastSeq)
	})
}

var dbErrorOnce sync.Once
var globalDBError *sqlx.DB

func initDBWithError() *sqlx.DB {
	dbErrorOnce.Do(func() {
		var err error
		globalDBError, err = sqlx.Open(
			"mysql",
			"root:1@tcp(localhost:3307)/cache_inv?parseTime=true&multiStatements=true",
		)
		if err != nil {
			panic(err)
		}
	})
	return globalDBError
}

func newRepoTestWithError() *repoTest {
	db := initDBWithError()

	return &repoTest{
		ctx:  context.Background(),
		db:   db,
		repo: NewRepository(db),
	}
}

func TestRepo_Repo_WithError(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		r := newRepoTestWithError()

		var err error

		_, err = r.repo.GetLastEvents(r.ctx, 16)
		assert.Error(t, err)

		_, err = r.repo.GetUnprocessedEvents(r.ctx, 16)
		assert.Error(t, err)

		_, err = r.repo.GetEventsFrom(r.ctx, 100, 16)
		assert.Error(t, err)

		err = r.repo.UpdateSequences(r.ctx, []cacheinv.InvalidateEvent{
			{
				ID:   1,
				Seq:  newInt64(11),
				Data: "Key01",
			},
		})
		assert.Error(t, err)

		_, err = r.repo.GetMinSequence(r.ctx)
		assert.Error(t, err)

		err = r.repo.DeleteEventsBefore(r.ctx, 100)
		assert.Error(t, err)

		_, err = r.repo.GetLastSequence(r.ctx, "server01")
		assert.Error(t, err)

		err = r.repo.SetLastSequence(r.ctx, "server01", 70)
		assert.Error(t, err)
	})
}
