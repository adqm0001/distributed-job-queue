package worker

import "github.com/adqm0001/distributed-job-queue/internal/job"
import "sync"

type DeadLetter struct {
	mu       sync.Mutex
	deadJobs []*job.Job
}

func (d *DeadLetter) Add(j *job.Job) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.deadJobs = append(d.deadJobs, j)
}

func (d *DeadLetter) List() []*job.Job {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]*job.Job, len(d.deadJobs))
	copy(out, d.deadJobs)
	return out
}
