package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"job-distributed/internal/jobs"
)

func main() {
	log.SetOutput(os.Stdout)

	if err := run(); err != nil {
		log.Println("application stopped:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	repo, queue, err := buildStorage(ctx)
	if err != nil {
		return err
	}
	defer repo.Close()
	defer queue.Close()

	dispatcher := jobs.NewDispatcher()
	worker := jobs.NewWorker(repo, dispatcher, queue)
	scheduler := jobs.NewScheduler(repo, worker)
	service := jobs.NewService(repo, worker)

	jobs.RegisterSampleHandlers(dispatcher)
	worker.SetConcurrencyLimit(jobs.JobType("deployment"), 1)

	mode := os.Getenv("APP_MODE")
	if mode == "" {
		mode = "demo"
	}

	switch mode {
	case "demo":
		return runDemo(ctx, service, worker, scheduler)
	case "producer":
		return runProducer(ctx, service)
	case "worker":
		return runWorker(ctx, worker, scheduler)
	default:
		return fmt.Errorf("unknown APP_MODE %q: use demo, producer, or worker", mode)
	}
}

func buildStorage(ctx context.Context) (jobs.JobRepository, jobs.JobQueue, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	rabbitURL := os.Getenv("RABBITMQ_URL")

	if (databaseURL == "") != (rabbitURL == "") {
		return nil, nil, errors.New("set both DATABASE_URL and RABBITMQ_URL, or leave both empty for in-memory mode")
	}

	if databaseURL == "" {
		log.Println("using in-memory repository and queue")
		return jobs.NewRepository(), jobs.NewMemoryQueue(100), nil
	}

	repo, err := jobs.NewPostgresRepository(ctx, databaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to postgres: %w", err)
	}

	queueName := os.Getenv("JOB_QUEUE_NAME")
	if queueName == "" {
		queueName = "jobs"
	}

	queue, err := jobs.NewRabbitMQQueue(rabbitURL, queueName, 3)
	if err != nil {
		repo.Close()
		return nil, nil, fmt.Errorf("connect to rabbitmq: %w", err)
	}

	log.Println("using PostgreSQL repository and RabbitMQ queue")
	return repo, queue, nil
}

func runDemo(ctx context.Context, service *jobs.Service, worker *jobs.Worker, scheduler *jobs.Scheduler) error {
	submittedJobs, err := submitSampleJobs(ctx, service)
	if err != nil {
		return err
	}

	logSubmittedJobs(submittedJobs)
	startWorkers(ctx, worker, scheduler)

	select {
	case <-time.After(3 * time.Second):
	case <-ctx.Done():
		return ctx.Err()
	}

	return printJobs(ctx, service)
}

func runProducer(ctx context.Context, service *jobs.Service) error {
	submittedJobs, err := submitSampleJobs(ctx, service)
	if err != nil {
		return err
	}

	logSubmittedJobs(submittedJobs)
	return nil
}

func runWorker(ctx context.Context, worker *jobs.Worker, scheduler *jobs.Scheduler) error {
	startWorkers(ctx, worker, scheduler)
	log.Println("worker service is ready")

	<-ctx.Done()
	log.Println("worker service is stopping")
	return nil
}

func startWorkers(ctx context.Context, worker *jobs.Worker, scheduler *jobs.Scheduler) {
	for workerID := 1; workerID <= 3; workerID++ {
		go worker.Start(ctx, workerID)
	}
	go scheduler.Start(ctx)
}

func submitSampleJobs(ctx context.Context, service *jobs.Service) ([]jobs.Job, error) {
	deploymentJob, err := service.SubmitJob(ctx, jobs.SubmitJobInput{
		Type:     jobs.JobType("deployment"),
		Priority: jobs.JobPriorityLow,
		Payload: map[string]string{
			"service": "billing-api",
			"version": "v1.0.0",
		},
		MaxRetries: 1,
	})
	if err != nil {
		return nil, err
	}

	secondDeploymentJob, err := service.SubmitJob(ctx, jobs.SubmitJobInput{
		Type:     jobs.JobType("deployment"),
		Priority: jobs.JobPriorityLow,
		Payload: map[string]string{
			"service": "orders-api",
			"version": "v2.1.0",
		},
	})
	if err != nil {
		return nil, err
	}

	emailJob, err := service.SubmitJob(ctx, jobs.SubmitJobInput{
		Type:     jobs.JobType("email"),
		Priority: jobs.JobPriorityHigh,
		Payload: map[string]string{
			"to":      "dev@example.com",
			"subject": "Welcome",
			"body":    "Your account is ready.",
		},
		MaxRetries: 3,
	})
	if err != nil {
		return nil, err
	}

	scheduledAt := time.Now().Add(2 * time.Second)
	webhookJob, err := service.SubmitJob(ctx, jobs.SubmitJobInput{
		Type:        jobs.JobType("webhook"),
		Priority:    jobs.JobPriorityMedium,
		ScheduledAt: &scheduledAt,
		Payload: map[string]string{
			"url": "https://example.com/hooks/job-finished",
		},
		MaxRetries: 2,
	})
	if err != nil {
		return nil, err
	}

	return []jobs.Job{emailJob, deploymentJob, secondDeploymentJob, webhookJob}, nil
}

func logSubmittedJobs(submittedJobs []jobs.Job) {
	jobIDs := make([]int, 0, len(submittedJobs))
	for _, job := range submittedJobs {
		jobIDs = append(jobIDs, job.ID)
	}
	log.Println("submitted jobs:", jobIDs)
}

func printJobs(ctx context.Context, service *jobs.Service) error {
	allJobs, err := service.ListJobs(ctx)
	if err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("Final job states:")
	for _, job := range allJobs {
		fmt.Printf("- job %d | type=%s | priority=%s | status=%s | enqueued=%t | attempts=%d | max_retries=%d", job.ID, job.Type, job.Priority, job.Status, job.Enqueued, job.Attempts, job.MaxRetries)
		if job.ScheduledAt != nil {
			fmt.Printf(" | scheduled_at=%s", job.ScheduledAt.Format(time.RFC3339))
		}
		if job.Error != "" {
			fmt.Printf(" | error=%s", job.Error)
		}
		fmt.Println()
	}

	return nil
}
