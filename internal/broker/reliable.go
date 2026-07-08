package broker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/adqm0001/distributed-job-queue/internal/job"
)

type RedisReliable struct {
	client  *redis.Client
	ctx     context.Context
	cancel  context.CancelFunc
	pending string
	active  string
}

func NewRedisReliable(addr string) *RedisReliable {
	ctx, cancel := context.WithCancel(context.Background())
	return &RedisReliable{
		client:  redis.NewClient(&redis.Options{Addr: addr}),
		ctx:     ctx,
		cancel:  cancel,
		pending: "pending",
		active:  "active",
	}
}

func taskKey(id string) string {
	return "task:" + id
}

func (r *RedisReliable) Submit(j *job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	if err := r.client.Set(r.ctx, taskKey(j.ID), data, 0).Err(); err != nil {
		return err
	}
	return r.client.LPush(r.ctx, r.pending, j.ID).Err()
}

var reserveScript = redis.NewScript(`
local id = redis.call('RPOP', KEYS[1])
if not id then
  return nil
end
local now = redis.call('TIME')[1]
redis.call('ZADD', KEYS[2], now, id)
return redis.call('GET', 'task:' .. id)
`)

func (r *RedisReliable) Dequeue() (*job.Job, error) {
	for {
		res, err := reserveScript.Run(r.ctx, r.client, []string{r.pending, r.active}).Result()
		if err == redis.Nil {
			if r.ctx.Err() != nil {
				return nil, r.ctx.Err()
			}
			time.Sleep(time.Second)
			continue
		}
		if err != nil {
			return nil, err
		}
		data, ok := res.(string)
		if !ok {
			return nil, redis.Nil
		}
		var j job.Job
		if err := json.Unmarshal([]byte(data), &j); err != nil {
			return nil, err
		}
		return &j, nil
	}
}

func (r *RedisReliable) Ack(j *job.Job) error {
	if err := r.client.ZRem(r.ctx, r.active, j.ID).Err(); err != nil {
		return err
	}
	return r.client.Del(r.ctx, taskKey(j.ID)).Err()
}

var reapScript = redis.NewScript(`
local now = redis.call('TIME')[1]
local cutoff = now - tonumber(ARGV[1])
local ids = redis.call('ZRANGE', KEYS[2], 0, cutoff, 'BYSCORE')
for i = 1, #ids do
  redis.call('LPUSH', KEYS[1], ids[i])
  redis.call('ZREM', KEYS[2], ids[i])
end
return #ids
`)

func (r *RedisReliable) Reap(timeout time.Duration) (int, error) {
	seconds := int64(timeout.Seconds())
	n, err := reapScript.Run(r.ctx, r.client, []string{r.pending, r.active}, seconds).Result()
	if err != nil {
		return 0, err
	}
	count, _ := n.(int64)
	return int(count), nil
}

func (r *RedisReliable) Close() error {
	r.cancel()
	return nil
}
