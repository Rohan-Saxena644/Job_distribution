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

	scheduledAt := time.Now().Add(1 * time.Hour)
	webhookJob := service.SubmitJob(jobs.SubmitJobInput{
		Type:        jobs.JobType("webhook"),
		ScheduledAt: &scheduledAt,
		Payload: map[string]string{
			"url": "https://example.com/hooks/job-finished",
		},
		MaxRetries: 2,
	})

	log.Println("submitted jobs:", emailJob.ID, deploymentJob.ID, webhookJob.ID)

	time.Sleep(400 * time.Millisecond)

	fmt.Println()
	fmt.Println("Final job states:")
	for _, job := range service.ListJobs() {
		fmt.Printf("- job %d | type=%s | status=%s", job.ID, job.Type, job.Status)
		if job.ScheduledAt != nil {
			fmt.Printf(" | scheduled_at=%s", job.ScheduledAt.Format(time.RFC3339))
		}
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
