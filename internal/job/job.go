package job

import "github.com/google/uuid"

type Status string

const (
	Pending Status = "pending"
	Running Status = "running"
	Done    Status = "done"
	Dead    Status = "dead"
)

type Priority int 

const (
	High Priority = 2 
	Medium Priority = 1 
	Low Priority = 0 
) 

type Job struct {
	ID       string
	Attempts int
	Kind     string
	Payload  []byte
	State    Status
	Priority Priority
}

func NewJob(kind string, payload []byte, priority Priority) *Job {
	var j Job
	j.ID = uuid.NewString()
	j.Kind = kind
	j.Payload = payload
	j.State = Pending
	j.Priority = priority
	return &j
}
