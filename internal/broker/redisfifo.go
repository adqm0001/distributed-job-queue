package broker

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"

	"github.com/adqm0001/distributed-job-queue/internal/job"
)

type RedisFIFO struct {
	client *redis.Client
	key    string
}

func NewRedisFIFO(addr, key string) RedisClient {
	return &RedisFIFO{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		key:    key,
	}
}

func (r *RedisFIFO) Submit(j *job.Job) error {
	ctx := context.Background()
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}

	return r.client.LPush(ctx, r.key, data).Err()
}

func (r *RedisFIFO) Dequeue() (*job.Job, error) {
	ctx := context.Background()

	res, err := r.client.BRPop(ctx, 0, r.key).Result()
	if err != nil {
		return nil, err
	}
	var j job.Job
	unerr := json.Unmarshal([]byte(res[1]), &j)
	if unerr != nil {
		return nil, unerr
	}

	return &j, err
}
