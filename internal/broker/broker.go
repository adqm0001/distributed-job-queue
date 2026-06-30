package broker

import "github.com/adqm0001/distributed-job-queue/internal/job"

type RedisClient interface {
	Submit(j *job.Job) error
	Dequeue() (*job.Job, error)
}
