package main

import (
	"fmt"
	"log"

	"github.com/adqm0001/distributed-job-queue/internal/broker"
	"github.com/adqm0001/distributed-job-queue/internal/job"
)

func main() {
	client := broker.NewRedisFIFO("localhost:6379", "jobs")

	for i := 1; i <= 10; i++ {
		err := client.Submit(job.NewJob("print", []byte(fmt.Sprintf("job-%d", i)), job.Medium))
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("submitted 10 jobs")
}
