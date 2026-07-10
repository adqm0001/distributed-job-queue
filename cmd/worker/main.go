package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/adqm0001/distributed-job-queue/internal/broker"
	"github.com/adqm0001/distributed-job-queue/internal/idempotency"
	"github.com/adqm0001/distributed-job-queue/internal/job"
	"github.com/adqm0001/distributed-job-queue/internal/worker"
)

func main() {
	name, _ := os.Hostname()
	if len(os.Args) > 1 {
		name = os.Args[1]
	}

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := broker.NewRedisReliable(addr)
	pool := worker.NewPool(client)
	rdb := redis.NewClient(&redis.Options{Addr: addr})

	pool.Register("print", func(j *job.Job) error {
		fmt.Printf("[%s] processing %s\n", name, string(j.Payload))
		return nil
	})

	pool.Register("charge", idempotency.Wrap(rdb,
		func(j *job.Job) string { return string(j.Payload) },
		time.Minute, 24*time.Hour,
		func(j *job.Job) error {
			fmt.Printf("[%s] charging %s\n", name, string(j.Payload))
			return nil
		},
	))

	pool.Start(3)
	fmt.Printf("[%s] started, waiting for jobs\n", name)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n, err := client.Reap(5 * time.Minute)
				if err != nil {
					log.Println(err)
				} else if n > 0 {
					log.Printf("[%s] reaped %d stuck jobs\n", name, n)
				}
			}
		}
	}()

	<-ctx.Done()

	fmt.Printf("[%s] shutting down\n", name)
	pool.Stop()
	fmt.Printf("[%s] done\n", name)
}
