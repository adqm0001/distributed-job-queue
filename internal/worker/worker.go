package worker

import "github.com/adqm0001/distributed-job-queue/internal/queue"
import "github.com/adqm0001/distributed-job-queue/internal/job"
import "time"
import "math"

type Handler func(payload []byte) error

const maxAttempts = 3

type Pool struct {
	queue    *queue.Queue
	handlers map[string]Handler
	dead     *DeadLetter
}

func backoff(attempts int) time.Duration {
	return time.Duration(math.Pow(2, float64(attempts-1))) * time.Second
}

func NewPool(q *queue.Queue) *Pool {
	return &Pool{queue: q, handlers: make(map[string]Handler), dead: &DeadLetter{}}
}

func (p *Pool) Register(kind string, h Handler) {
	p.handlers[kind] = h
}

func (p *Pool) DeadJobs() []*job.Job {
	return p.dead.List()
}

func (p *Pool) work() {
	for {
		j := p.queue.Dequeue()
		handler := p.handlers[j.Kind]

		if handler == nil {
			continue
		}

		err := handler(j.Payload)

		if err == nil {
			j.State = job.Done
			continue
		}

		j.Attempts++

		if j.Attempts >= maxAttempts {
			j.State = job.Dead
			p.dead.Add(j)
		} else {
			go func() {
				time.Sleep(backoff(j.Attempts))
				p.queue.Submit(j)
			}()
		}
	}
}

func (p *Pool) Start(workerCount int) {
	for i := 0; i < workerCount; i++ {
		go p.work()
	}
}
