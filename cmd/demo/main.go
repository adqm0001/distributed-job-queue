package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/adqm0001/distributed-job-queue/internal/job"
	"github.com/adqm0001/distributed-job-queue/internal/policy"
	"github.com/adqm0001/distributed-job-queue/internal/queue"
	"github.com/adqm0001/distributed-job-queue/internal/worker"
)

func handlePrint(payload []byte) error {
	fmt.Println("processing:", string(payload))
	return nil
}

func main() {
	q := queue.NewQueue(&policy.PriorityQueue{})
	pool := worker.NewPool(q)
	pool.Register("print", handlePrint)
	pool.Start(3)

	q.Submit(job.NewJob("print", []byte("low job"), job.Low))
	q.Submit(job.NewJob("print", []byte("high job"), job.High))
	q.Submit(job.NewJob("print", []byte("medium job"), job.Medium))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	<-ctx.Done()
	fmt.Println("shutting down...")
	pool.Stop()
	fmt.Println("done")
}
