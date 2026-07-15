package policy

import (
	"testing"

	"github.com/adqm0001/distributed-job-queue/internal/job"
)

func TestWeightedFairRatio(t *testing.T) {
	weighted := &WeightedFair{}

	for i := 0; i < 10; i++ {
		weighted.Add(job.NewJob("test", []byte("x"), job.High))
		weighted.Add(job.NewJob("test", []byte("x"), job.Medium))
		weighted.Add(job.NewJob("test", []byte("x"), job.Low))
	}

	got := map[job.Priority]int{}
	for i := 0; i < 6; i++ {
		next := weighted.Next()
		if next == nil {
			t.Fatalf("Next returned nil on call %d, want a job", i+1)
		}
		got[next.Priority]++
	}

	want := map[job.Priority]int{job.High: 3, job.Medium: 2, job.Low: 1}
	for prio, n := range want {
		if got[prio] != n {
			t.Errorf("priority %d: got %d per cycle, want %d", prio, got[prio], n)
		}
	}
}

func TestWeightedFairNoStarvation(t *testing.T) {
	weighted := &WeightedFair{}

	const perPriority = 20
	for i := 0; i < perPriority; i++ {
		weighted.Add(job.NewJob("test", []byte("x"), job.High))
		weighted.Add(job.NewJob("test", []byte("x"), job.Medium))
		weighted.Add(job.NewJob("test", []byte("x"), job.Low))
	}

	got := map[job.Priority]int{}
	for {
		next := weighted.Next()
		if next == nil {
			break
		}
		got[next.Priority]++
	}

	for _, prio := range []job.Priority{job.High, job.Medium, job.Low} {
		if got[prio] != perPriority {
			t.Errorf("priority %d: drained %d jobs, want %d", prio, got[prio], perPriority)
		}
	}

	if weighted.Len() != 0 {
		t.Errorf("Len() = %d after draining, want 0", weighted.Len())
	}
}
