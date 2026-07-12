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
	scheduler := jobs.NewScheduler(repo, worker)
	service := jobs.NewService(repo, worker)

	jobs.RegisterSampleHandlers(dispatcher)

	deploymentJob := service.SubmitJob(jobs.SubmitJobInput{
		Type:     jobs.JobType("deployment"),
		Priority: jobs.JobPriorityLow,
		Payload: map[string]string{
			"service": "billing-api",
			"version": "v1.0.0",
		},
		MaxRetries: 1,
	})

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

	scheduledAt := time.Now().Add(2 * time.Second)
	webhookJob := service.SubmitJob(jobs.SubmitJobInput{
		Type:        jobs.JobType("webhook"),
		Priority:    jobs.JobPriorityMedium,
		ScheduledAt: &scheduledAt,
		Payload: map[string]string{
			"url": "https://example.com/hooks/job-finished",
		},
		MaxRetries: 2,
	})

	log.Println("submitted jobs:", emailJob.ID, deploymentJob.ID, webhookJob.ID)

	go worker.Start()
	go scheduler.Start()

	time.Sleep(3 * time.Second)

	fmt.Println()
	fmt.Println("Final job states:")
	for _, job := range service.ListJobs() {
		fmt.Printf("- job %d | type=%s | priority=%s | status=%s | enqueued=%t | attempts=%d | max_retries=%d", job.ID, job.Type, job.Priority, job.Status, job.Enqueued, job.Attempts, job.MaxRetries)
		if job.ScheduledAt != nil {
			fmt.Printf(" | scheduled_at=%s", job.ScheduledAt.Format(time.RFC3339))
		}
		if job.Error != "" {
			fmt.Printf(" | error=%s", job.Error)
		}
		fmt.Println()
	}
}
