package broker

import (
	"encoding/json"
	"time"

	"github.com/adqm0001/distributed-job-queue/internal/job"
	"github.com/redis/go-redis/v9"
)

func (r *RedisReliable) Schedule(j *job.Job, delay time.Duration) error {
	now, err := r.client.Time(r.ctx).Result()

	if err != nil {
		return err
	}

	data, err := json.Marshal(j)
	if err != nil {
		return err
	}

	if err = r.client.HSet(r.ctx, "task:"+j.ID,
		"data", data,
		"state", string(job.Scheduled),
		"attempts", 0,
	).Err(); err != nil {
		return err
	}

	return r.client.ZAdd(r.ctx, r.scheduled, redis.Z{
		Score:  float64(now.Add(delay).Unix()),
		Member: j.ID,
	}).Err()
}

var promoteScript = redis.NewScript(`
local now = redis.call('TIME')[1]
local ids = redis.call('ZRANGE', KEYS[1], '-inf', now, 'BYSCORE')
for i = 1, #ids do
  local id = ids[i]
  local tk = 'task:' .. id
	redis.call('ZREM', KEYS[1], id)
	redis.call('LPUSH', KEYS[2], id)
	redis.call('HSET', tk, 'state', 'pending')
end 

return #ids
`)

func (r *RedisReliable) PromoteDue() (int, error) {
	res, err := promoteScript.Run(r.ctx, r.client, []string{r.scheduled, r.pending}).Int()

	if err != nil {
		return 0, err
	}

	return res, nil
}
