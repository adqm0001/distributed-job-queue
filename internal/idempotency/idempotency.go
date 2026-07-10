package idempotency

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/adqm0001/distributed-job-queue/internal/job"
	"github.com/adqm0001/distributed-job-queue/internal/worker"
)

var ErrInProgress = errors.New("operation already in progress")

var releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
end
return 1
`)

func Wrap(rdb *redis.Client, key func(*job.Job) string, lease, retention time.Duration, h worker.Handler) worker.Handler {
	return func(j *job.Job) error {
		ctx := context.Background()
		k := "processed:" + key(j)

		claimed, err := rdb.SetNX(ctx, k, j.ID, lease).Result()
		if err != nil {
			return err
		}

		if !claimed {
			state, err := rdb.Get(ctx, k).Result()
			if err == redis.Nil {
				return ErrInProgress
			}
			if err != nil {
				return err
			}
			if state == "done" {
				return nil
			}
			return ErrInProgress
		}

		if err := h(j); err != nil {
			releaseScript.Run(ctx, rdb, []string{k}, j.ID)
			return err
		}

		return rdb.Set(ctx, k, "done", retention).Err()
	}
}
