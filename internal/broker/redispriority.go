package broker

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"

	"github.com/adqm0001/distributed-job-queue/internal/job"
)

type RedisPriority struct {
	client *redis.Client
	key    string
}

func NewRedisPriority(addr, key string) RedisClient {
	return &RedisPriority{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		key:    key,
	}
}

func (r *RedisPriority) Submit(j *job.Job) error {
	ctx := context.Background()
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}

	return r.client.ZAdd(ctx, r.key, redis.Z{
		Member: data,
		Score:  float64(j.Priority),
	}).Err()
}

func (r *RedisPriority) Dequeue() (*job.Job, error) {
	ctx := context.Background()

	res, err := r.client.BZPopMax(ctx, 0, r.key).Result()
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
