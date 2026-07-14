package jobs

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

type JobEvent struct {
	Name       string    `json:"name"`
	JobID      int       `json:"job_id"`
	Type       JobType   `json:"type"`
	Status     JobStatus `json:"status"`
	Attempts   int       `json:"attempts"`
	Error      string    `json:"error,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
}

type EventPublisher interface {
	Publish(ctx context.Context, event JobEvent) error
	Close() error
}

type NoopEventPublisher struct{}

func (p *NoopEventPublisher) Publish(ctx context.Context, event JobEvent) error {
	return nil
}

func (p *NoopEventPublisher) Close() error {
	return nil
}

type RedisEventPublisher struct {
	client  *redis.Client
	channel string
}

func NewRedisEventPublisher(ctx context.Context, redisURL string, channel string) (*RedisEventPublisher, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(options)
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, err
	}

	return &RedisEventPublisher{
		client:  client,
		channel: channel,
	}, nil
}

func (p *RedisEventPublisher) Publish(ctx context.Context, event JobEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return p.client.Publish(ctx, p.channel, payload).Err()
}

func (p *RedisEventPublisher) Close() error {
	return p.client.Close()
}
