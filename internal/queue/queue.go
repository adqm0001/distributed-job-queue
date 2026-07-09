package queue

import (
	"sync"

	"github.com/adqm0001/distributed-job-queue/internal/job"
	"github.com/adqm0001/distributed-job-queue/internal/policy"
)

type Queue struct {
	mu      sync.Mutex
	policy  policy.SchedulingPolicy
	cond    *sync.Cond
	closing bool
}

func NewQueue(p policy.SchedulingPolicy) *Queue {
	q := &Queue{policy: p}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *Queue) Submit(j *job.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closing {
		return nil
	}

	q.policy.Add(j)
	q.cond.Signal()
	return nil
}

func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closing = true
	q.cond.Broadcast()
	return nil
}

func (q *Queue) Ack(j *job.Job) error {
	return nil
}

func (q *Queue) Nack(j *job.Job) error {
	return q.Submit(j)
}

func (q *Queue) Dequeue() (*job.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.policy.Len() == 0 && !q.closing {
		q.cond.Wait()
	}

	if q.policy.Len() == 0 && q.closing {
		return nil, nil
	}

	return q.policy.Next(), nil
}
