// Package stream is the Redis Streams delivery path (FR-10, tdd.md §4.7).
// SQLite remains the source of truth; this package only carries job IDs.
package stream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	name  = "forge:jobs"
	group = "forge"
)

// ErrEmpty means no message was available before the block timed out.
var ErrEmpty = errors.New("stream: empty")

// Stream is the forge:jobs Redis stream and its consumer group.
type Stream struct {
	rdb *redis.Client
}

// Message is one stream entry: Redis ID and the Forge job ID.
type Message struct {
	ID    string
	JobID string
}

// Open pings addr and ensures the stream and consumer group exist.
func Open(ctx context.Context, addr string) (*Stream, error) {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, fmt.Errorf("stream: ping %s: %w", addr, err)
	}
	s := &Stream{rdb: rdb}
	if err := s.ensure(ctx); err != nil {
		rdb.Close()
		return nil, err
	}
	return s, nil
}

func (s *Stream) ensure(ctx context.Context) error {
	err := s.rdb.XGroupCreateMkStream(ctx, name, group, "0").Err()
	if err != nil && !isBusyGroup(err) {
		return fmt.Errorf("stream: xgroup: %w", err)
	}
	return nil
}

func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// Close closes the Redis client.
func (s *Stream) Close() error {
	return s.rdb.Close()
}

// Add XADDs jobID. Call after the SQLite insert (FR-10, NFR-3).
func (s *Stream) Add(ctx context.Context, jobID string) error {
	_, err := s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: name,
		Values: map[string]any{"id": jobID},
	}).Result()
	if err != nil {
		return fmt.Errorf("stream: xadd: %w", err)
	}
	return nil
}

// Claim reads one pending entry for workerID, or blocks up to wait for a new one.
func (s *Stream) Claim(ctx context.Context, workerID string, wait time.Duration) (*Message, error) {
	msg, err := s.read(ctx, workerID, "0", 0)
	if err == nil {
		return msg, nil
	}
	if !errors.Is(err, ErrEmpty) {
		return nil, err
	}
	return s.read(ctx, workerID, ">", wait)
}

func (s *Stream) read(ctx context.Context, workerID, id string, block time.Duration) (*Message, error) {
	res, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: workerID,
		Streams:  []string{name, id},
		Count:    1,
		Block:    block,
	}).Result()
	if err == redis.Nil || errors.Is(err, context.DeadlineExceeded) {
		return nil, ErrEmpty
	}
	if err != nil {
		return nil, fmt.Errorf("stream: xreadgroup: %w", err)
	}
	if len(res) == 0 || len(res[0].Messages) == 0 {
		return nil, ErrEmpty
	}
	m := res[0].Messages[0]
	jobID, _ := m.Values["id"].(string)
	if jobID == "" {
		return nil, fmt.Errorf("stream: missing id in %s", m.ID)
	}
	return &Message{ID: m.ID, JobID: jobID}, nil
}

// Ack removes msgID from the consumer group's pending list.
func (s *Stream) Ack(ctx context.Context, msgID string) error {
	if err := s.rdb.XAck(ctx, name, group, msgID).Err(); err != nil {
		return fmt.Errorf("stream: xack: %w", err)
	}
	return nil
}

// Has reports whether jobID already appears in the stream.
func (s *Stream) Has(ctx context.Context, jobID string) (bool, error) {
	msgs, err := s.rdb.XRange(ctx, name, "-", "+").Result()
	if err != nil {
		return false, fmt.Errorf("stream: xrange: %w", err)
	}
	for _, m := range msgs {
		if id, _ := m.Values["id"].(string); id == jobID {
			return true, nil
		}
	}
	return false, nil
}

const sweeperConsumer = "sweeper"

// AutoClaim takes pending entries idle longer than minIdle (FR-11).
func (s *Stream) AutoClaim(ctx context.Context, minIdle time.Duration) ([]Message, error) {
	msgs, _, err := s.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   name,
		Group:    group,
		Consumer: sweeperConsumer,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    100,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("stream: xautoclaim: %w", err)
	}
	out := make([]Message, 0, len(msgs))
	for _, m := range msgs {
		jobID, _ := m.Values["id"].(string)
		if jobID == "" {
			continue
		}
		out = append(out, Message{ID: m.ID, JobID: jobID})
	}
	return out, nil
}

// PendingFor returns PEL entries held by workerID.
func (s *Stream) PendingFor(ctx context.Context, workerID string) ([]Message, error) {
	pending, err := s.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   name,
		Group:    group,
		Start:    "-",
		End:      "+",
		Count:    100,
		Consumer: workerID,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("stream: xpending: %w", err)
	}
	out := make([]Message, 0, len(pending))
	for _, p := range pending {
		msgs, err := s.rdb.XRange(ctx, name, p.ID, p.ID).Result()
		if err != nil {
			return nil, fmt.Errorf("stream: xrange %s: %w", p.ID, err)
		}
		if len(msgs) == 0 {
			continue
		}
		jobID, _ := msgs[0].Values["id"].(string)
		if jobID == "" {
			continue
		}
		out = append(out, Message{ID: p.ID, JobID: jobID})
	}
	return out, nil
}

// Reconcile XADDs queued job IDs that are missing from the stream (tdd.md §6.2).
func (s *Stream) Reconcile(ctx context.Context, queued []string) error {
	for _, id := range queued {
		ok, err := s.Has(ctx, id)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		if err := s.Add(ctx, id); err != nil {
			return err
		}
	}
	return nil
}
