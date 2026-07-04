package broker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/adqm0001/distributed-job-queue/internal/job"
)

type RedisPriority struct {
	client *redis.Client
	key    string
	ctx    context.Context
	cancel context.CancelFunc
}

func NewRedisPriority(addr, key string) RedisClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &RedisPriority{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		key:    key,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (r *RedisPriority) Submit(j *job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}

	return r.client.ZAdd(r.ctx, r.key, redis.Z{
		Member: data,
		Score:  float64(j.Priority),
	}).Err()
}

func (r *RedisPriority) Dequeue() (*job.Job, error) {
	for {
		res, err := r.client.BZPopMax(r.ctx, time.Second, r.key).Result()
		if err == redis.Nil {
			if r.ctx.Err() != nil {
				return nil, r.ctx.Err()
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		value := res.Member.(string)
		var j job.Job
		if err := json.Unmarshal([]byte(value), &j); err != nil {
			return nil, err
		}
		return &j, nil
	}
}

func (r *RedisPriority) Close() error {
	r.cancel()
	return nil
}
