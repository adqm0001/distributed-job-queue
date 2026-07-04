package broker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/adqm0001/distributed-job-queue/internal/job"
)

type RedisFIFO struct {
	client *redis.Client
	key    string
	ctx    context.Context
	cancel context.CancelFunc
}

func NewRedisFIFO(addr, key string) RedisClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &RedisFIFO{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		key:    key,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (r *RedisFIFO) Submit(j *job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}

	return r.client.LPush(r.ctx, r.key, data).Err()
}

func (r *RedisFIFO) Dequeue() (*job.Job, error) {
	for {
		res, err := r.client.BRPop(r.ctx, time.Second, r.key).Result()
		if err == redis.Nil {
			if r.ctx.Err() != nil {
				return nil, r.ctx.Err()
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		var j job.Job
		if err := json.Unmarshal([]byte(res[1]), &j); err != nil {
			return nil, err
		}
		return &j, nil
	}
}

func (r *RedisFIFO) Close() error {
	r.cancel()
	return nil
}
