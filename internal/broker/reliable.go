package broker

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/adqm0001/distributed-job-queue/internal/job"
)

type RedisReliable struct {
	client      *redis.Client
	ctx         context.Context
	cancel      context.CancelFunc
	pending     string
	active      string
	dead        string
	maxAttempts int
}

func NewRedisReliable(addr string) *RedisReliable {
	ctx, cancel := context.WithCancel(context.Background())
	return &RedisReliable{
		client:      redis.NewClient(&redis.Options{Addr: addr}),
		ctx:         ctx,
		cancel:      cancel,
		pending:     "pending",
		active:      "active",
		dead:        "dead",
		maxAttempts: 3,
	}
}

func (r *RedisReliable) Submit(j *job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	if err := r.client.HSet(r.ctx, "task:"+j.ID,
		"data", data,
		"state", string(job.Pending),
		"attempts", 0,
	).Err(); err != nil {
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
redis.call('HSET', 'task:' .. id, 'state', 'active')
return id
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
		id, ok := res.(string)
		if !ok {
			return nil, redis.Nil
		}
		vals, err := r.client.HMGet(r.ctx, "task:"+id, "data", "attempts").Result()
		if err != nil {
			return nil, err
		}
		data, _ := vals[0].(string)
		var j job.Job
		if err := json.Unmarshal([]byte(data), &j); err != nil {
			return nil, err
		}
		if a, ok := vals[1].(string); ok {
			j.Attempts, _ = strconv.Atoi(a)
		}
		return &j, nil
	}
}

func (r *RedisReliable) Ack(j *job.Job) error {
	if err := r.client.ZRem(r.ctx, r.active, j.ID).Err(); err != nil {
		return err
	}
	return r.client.Del(r.ctx, "task:"+j.ID).Err()
}

var failScript = redis.NewScript(`
local id = ARGV[1]
local tk = 'task:' .. id
local attempts = redis.call('HINCRBY', tk, 'attempts', 1)
redis.call('ZREM', KEYS[2], id)
if attempts >= tonumber(ARGV[2]) then
  redis.call('HSET', tk, 'state', 'dead')
  redis.call('LPUSH', KEYS[3], id)
else
  redis.call('HSET', tk, 'state', 'pending')
  redis.call('LPUSH', KEYS[1], id)
end
return attempts
`)

func (r *RedisReliable) Nack(j *job.Job) error {
	return failScript.Run(r.ctx, r.client,
		[]string{r.pending, r.active, r.dead},
		j.ID, r.maxAttempts,
	).Err()
}

var reapScript = redis.NewScript(`
local now = redis.call('TIME')[1]
local cutoff = now - tonumber(ARGV[1])
local ids = redis.call('ZRANGE', KEYS[2], 0, cutoff, 'BYSCORE')
local max = tonumber(ARGV[2])
for i = 1, #ids do
  local id = ids[i]
  local tk = 'task:' .. id
  local attempts = redis.call('HINCRBY', tk, 'attempts', 1)
  redis.call('ZREM', KEYS[2], id)
  if attempts >= max then
    redis.call('HSET', tk, 'state', 'dead')
    redis.call('LPUSH', KEYS[3], id)
  else
    redis.call('HSET', tk, 'state', 'pending')
    redis.call('LPUSH', KEYS[1], id)
  end
end
return #ids
`)

func (r *RedisReliable) Reap(timeout time.Duration) (int, error) {
	seconds := int64(timeout.Seconds())
	n, err := reapScript.Run(r.ctx, r.client,
		[]string{r.pending, r.active, r.dead},
		seconds, r.maxAttempts,
	).Result()
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
