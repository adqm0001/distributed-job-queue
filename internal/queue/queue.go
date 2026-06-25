package queue

import (
	"sync"

	"github.com/adqm0001/distributed-job-queue/internal/job"
	"github.com/adqm0001/distributed-job-queue/internal/policy"
)

type Queue struct {
	mu     sync.Mutex
	policy policy.SchedulingPolicy
	cond   *sync.Cond
}

func NewQueue(p policy.SchedulingPolicy) *Queue {
	q := &Queue{policy: p}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *Queue) Submit(j *job.Job) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.policy.Add(j)
	q.cond.Signal()
}

func (q *Queue) Dequeue() *job.Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.policy.Len() == 0 {
		q.cond.Wait()
	}
	return q.policy.Next()
}
