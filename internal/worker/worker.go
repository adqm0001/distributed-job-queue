package worker

import "github.com/adqm0001/distributed-job-queue/internal/queue"

type Handler func(payload []byte) error

type Pool struct {
	queue    *queue.Queue
	handlers map[string]Handler
}

func NewPool(q *queue.Queue) *Pool {
	return &Pool{queue: q, handlers: make(map[string]Handler)}
}

func (p *Pool) Register(kind string, h Handler) {
	p.handlers[kind] = h
}

func (p *Pool) work() {
	for {
		j := p.queue.Dequeue()
		handler := p.handlers[j.Kind]

		if handler == nil {
			continue
		}

		handler(j.Payload)
	}
}

func (p *Pool) Start(workerCount int) {
	for i := 0; i < workerCount; i++ {
		go p.work()
	}
}
