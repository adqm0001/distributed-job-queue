package policy

import (
	"container/heap"

	"github.com/adqm0001/distributed-job-queue/internal/job"
)

type PriorityQueue struct {
	jobs []*job.Job
}

func (p *PriorityQueue) Len() int {
	return len(p.jobs)
}

func (p *PriorityQueue) Less(i, j int) bool {
	return p.jobs[i].Priority > p.jobs[j].Priority
}

func (p *PriorityQueue) Swap(i, j int) {
	p.jobs[i], p.jobs[j] = p.jobs[j], p.jobs[i]
}

func (p *PriorityQueue) Push(x any) {
	p.jobs = append(p.jobs, x.(*job.Job))
}

func (p *PriorityQueue) Pop() any {
	n := len(p.jobs)
	popped := p.jobs[n-1]
	p.jobs[n-1] = nil
	p.jobs = p.jobs[:n-1]
	return popped
}

func (p *PriorityQueue) Add(j *job.Job) {
	heap.Push(p, j)
}

func (p *PriorityQueue) Next() *job.Job {
	if p.Len() == 0 {
		return nil
	}
	return heap.Pop(p).(*job.Job)
}
