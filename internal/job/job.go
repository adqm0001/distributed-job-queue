package job

import "github.com/google/uuid"

type Status string

const (
	Pending Status = "pending"
	Running Status = "running"
	Done    Status = "done"
	Dead    Status = "dead"
)

type Job struct {
	ID       string
	Attempts int
	Kind     string
	Payload  []byte
	State    Status
}

func NewJob(kind string, payload []byte) *Job {
	var j Job
	j.ID = uuid.NewString()
	j.Kind = kind
	j.Payload = payload
	j.State = Pending
	return &j
}
