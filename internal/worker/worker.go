package worker

import (
	"context"
	"errors"
	"log"
	"sync"

	"github.com/adqm0001/distributed-job-queue/internal/job"
)

type Handler func(payload []byte) error

type Queue interface {
	Submit(j *job.Job) error
	Dequeue() (*job.Job, error)
	Ack(j *job.Job) error
	Nack(j *job.Job) error
	Close() error
}

type Pool struct {
	queue    Queue
	handlers map[string]Handler
	wg       sync.WaitGroup
}

func NewPool(q Queue) *Pool {
	return &Pool{queue: q, handlers: make(map[string]Handler)}
}

func (p *Pool) Register(kind string, h Handler) {
	p.handlers[kind] = h
}

func (p *Pool) work() {
	defer p.wg.Done()

	for {
		j, err := p.queue.Dequeue()

		if err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Println(err)
			}
			return
		}

		if j == nil {
			return
		}

		handler := p.handlers[j.Kind]

		if handler == nil {
			log.Printf("no handler for kind %q", j.Kind)
			if err := p.queue.Nack(j); err != nil {
				log.Println(err)
			}
			continue
		}

		if err := handler(j.Payload); err != nil {
			log.Println(err)
			if err := p.queue.Nack(j); err != nil {
				log.Println(err)
			}
			continue
		}

		if err := p.queue.Ack(j); err != nil {
			log.Println(err)
		}
	}
}

func (p *Pool) Start(workerCount int) {
	for i := 0; i < workerCount; i++ {
		p.wg.Add(1)
		go p.work()
	}
}

func (p *Pool) Stop() {
	p.queue.Close()
	p.wg.Wait()
}
