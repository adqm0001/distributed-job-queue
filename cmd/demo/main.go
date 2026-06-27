package main

import (
	"fmt"
	"time"

	"github.com/adqm0001/distributed-job-queue/internal/job"
	"github.com/adqm0001/distributed-job-queue/internal/policy"
	"github.com/adqm0001/distributed-job-queue/internal/queue"
	"github.com/adqm0001/distributed-job-queue/internal/worker"
)

// alwaysFails never succeeds, so we can watch retries + the dead-letter happen.
func alwaysFails(payload []byte) error {
	fmt.Println("attempting:", string(payload))
	return fmt.Errorf("boom")
}

func main() {
	q := queue.NewQueue(&policy.FIFO{})
	pool := worker.NewPool(q)

	pool.Register("flaky", alwaysFails)
	pool.Start(3)

	q.Submit(job.NewJob("flaky", []byte("job A")))

	// Wait for the retries (1s then 2s) to play out and the job to die.
	time.Sleep(5 * time.Second)

	fmt.Println("--- dead letter ---")
	for _, j := range pool.DeadJobs() {
		fmt.Printf("id=%s kind=%s attempts=%d state=%s\n", j.ID, j.Kind, j.Attempts, j.State)
	}
}
