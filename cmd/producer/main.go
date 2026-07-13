package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/adqm0001/distributed-job-queue/internal/broker"
	"github.com/adqm0001/distributed-job-queue/internal/job"
)

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := broker.NewRedisReliable(addr)

	for i := 1; i <= 10; i++ {
		err := client.Submit(job.NewJob("print", []byte(fmt.Sprintf("job-%d", i)), job.Medium))
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("submitted 10 jobs")

	err := client.Schedule(job.NewJob("print", []byte("delayed job"), job.Medium), 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("scheduled 1 job for 5s from now")
}
