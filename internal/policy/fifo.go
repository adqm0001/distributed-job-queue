package policy

import "github.com/adqm0001/distributed-job-queue/internal/job"

type FIFO struct {
	jobs []*job.Job
}

func (fifo *FIFO) Add(j *job.Job) {
	fifo.jobs = append(fifo.jobs, j)
}

func (fifo *FIFO) Next() *job.Job {
	if len(fifo.jobs) == 0 {
		return nil
	}
	next := fifo.jobs[0]
	fifo.jobs = fifo.jobs[1:]
	return next
}

func (fifo *FIFO) Len() int {
	return len(fifo.jobs)
}
