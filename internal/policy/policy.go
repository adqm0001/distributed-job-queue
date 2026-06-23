package policy

import "github.com/adqm0001/distributed-job-queue/internal/job"

type SchedulingPolicy interface {
	Add(j *job.Job)
	Next() *job.Job
}
