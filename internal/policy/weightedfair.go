package policy

import "github.com/adqm0001/distributed-job-queue/internal/job"

var defaultPattern = []job.Priority{
	job.High, job.High, job.High,
	job.Medium, job.Medium,
	job.Low,
}

type WeightedFair struct {
	buckets map[job.Priority][]*job.Job
	pattern []job.Priority
	i       int
}

func (w *WeightedFair) Add(j *job.Job) {
	if w.buckets == nil {
		w.buckets = make(map[job.Priority][]*job.Job)
	}

	if w.pattern == nil {
		w.pattern = defaultPattern
	}

	w.buckets[j.Priority] = append(w.buckets[j.Priority], j)
}

func (w *WeightedFair) Next() *job.Job {
	for range w.pattern {
		prio := w.pattern[w.i]
		w.i = (w.i + 1) % len(w.pattern)

		if len(w.buckets[prio]) > 0 {
			next := w.buckets[prio][0]
			w.buckets[prio] = w.buckets[prio][1:]
			return next
		}
	}

	return nil
}

func (w *WeightedFair) Len() int {
	n := 0
	for _, bucket := range w.buckets {
		n += len(bucket)
	}
	return n
}
