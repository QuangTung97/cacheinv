package mysql

import (
	"context"

	"github.com/QuangTung97/cacheinv"
)

type repoImpl struct {
}

var _ cacheinv.Repository = &repoImpl{}

func NewRepository() cacheinv.Repository {
	return &repoImpl{}
}

// GetLastEvents returns top *limit* events (events with the highest sequence numbers),
// by sequence number in ascending order, ignore events with null sequence number
func (r *repoImpl) GetLastEvents(ctx context.Context, limit uint64) ([]cacheinv.InvalidateEvent, error) {
	return nil, nil
}

// GetUnprocessedEvents returns list of events with the smallest event *id* (not sequence number)
// *AND* have NULL sequence numbers, in ascending order of event *id*
// size of the list is limited by *limit*
func (r *repoImpl) GetUnprocessedEvents(ctx context.Context, limit uint64) ([]cacheinv.InvalidateEvent, error) {
	return nil, nil
}

// GetEventsFrom returns list of events with sequence number >= *from*
// in ascending order of event sequence numbers, ignoring events with null sequence numbers
// size of the list is limited by *limit*
func (r *repoImpl) GetEventsFrom(ctx context.Context, from uint64, limit uint64) ([]cacheinv.InvalidateEvent, error) {
	return nil, nil
}

// UpdateSequences updates only sequence numbers of *events*
func (r *repoImpl) UpdateSequences(ctx context.Context, events []cacheinv.InvalidateEvent) error {
	return nil
}
