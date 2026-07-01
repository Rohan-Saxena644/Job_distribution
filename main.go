package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"job-distributed/internal/jobs"
)

func main() {
	log.SetOutput(os.Stdout)

	repo := jobs.NewRepository()
	dispatcher := jobs.NewDispatcher()
	worker := jobs.NewWorker(repo, dispatcher)
	service := jobs.NewService(repo, worker)

	registerHandlers(dispatcher)

	go worker.Start()

	emailJob := service.SubmitJob(jobs.SubmitJobInput{
		Type:     jobs.JobType("email"),
		Priority: jobs.JobPriorityHigh,
		Payload: map[string]string{
			"to":      "dev@example.com",
			"subject": "Welcome",
			"body":    "Your account is ready.",
		},
		MaxRetries: 3,
	})

	deploymentJob := service.SubmitJob(jobs.SubmitJobInput{
		Type: jobs.JobType("deployment"),
		Payload: map[string]string{
			"service": "billing-api",
			"version": "v1.0.0",
		},
		MaxRetries: 1,
	})

	log.Println("submitted jobs:", emailJob.ID, deploymentJob.ID)

	time.Sleep(400 * time.Millisecond)

	fmt.Println()
	fmt.Println("Final job states:")
	for _, job := range service.ListJobs() {
		fmt.Printf("- job %d | type=%s | status=%s", job.ID, job.Type, job.Status)
		if job.Error != "" {
			fmt.Printf(" | error=%s", job.Error)
		}
		fmt.Println()
	}
}

func registerHandlers(dispatcher *jobs.Dispatcher) {
	dispatcher.Register(jobs.JobType("email"), func(job jobs.Job) error {
		log.Println("sending email to", job.Payload["to"])
		time.Sleep(150 * time.Millisecond)
		return nil
	})

	dispatcher.Register(jobs.JobType("deployment"), func(job jobs.Job) error {
		log.Println("deploying service", job.Payload["service"], "version", job.Payload["version"])
		time.Sleep(100 * time.Millisecond)
		return fmt.Errorf("deployment failed for %s", job.Payload["service"])
	})
}
