package main

import (
	"fmt"
	"time"

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
	q := queue.NewQueue(&policy.FIFO{})
	pool := worker.NewPool(q)

	pool.Register("print", handlePrint)
	pool.Start(3)

	for i := 1; i <= 5; i++ {
		q.Submit(job.NewJob("print", []byte(fmt.Sprintf("job #%d", i))))
	}

	time.Sleep(time.Second)
}
