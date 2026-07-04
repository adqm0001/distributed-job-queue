package worker

import (
	"context"
	"errors"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/adqm0001/distributed-job-queue/internal/job"
)

type Handler func(payload []byte) error

type Queue interface {
	Submit(j *job.Job) error
	Dequeue() (*job.Job, error)
	Close() error
}

const maxAttempts = 3

type Pool struct {
	queue    Queue
	handlers map[string]Handler
	dead     *DeadLetter
	wg       sync.WaitGroup
}

func backoff(attempts int) time.Duration {
	exp := int64(math.Pow(2, float64(attempts-1))) * int64(time.Second)
	return time.Duration(rand.Int63n(exp))
}

func NewPool(q Queue) *Pool {
	return &Pool{queue: q, handlers: make(map[string]Handler), dead: &DeadLetter{}}
}

func (p *Pool) Register(kind string, h Handler) {
	p.handlers[kind] = h
}

func (p *Pool) DeadJobs() []*job.Job {
	return p.dead.List()
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
			continue
		}

		handleErr := handler(j.Payload)

		if handleErr == nil {
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
				err := p.queue.Submit(j)
				if err != nil {
					log.Println(err)
				}
			}()
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
