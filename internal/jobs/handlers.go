package jobs

import (
	"context"
	"fmt"
	"log"
	"time"
)

type EmailHandler struct{}

func (h EmailHandler) Execute(ctx context.Context, job Job) error {
	log.Println("sending email to", job.Payload["to"])
	time.Sleep(150 * time.Millisecond)
	return nil
}

type DeploymentHandler struct{}

func (h DeploymentHandler) Execute(ctx context.Context, job Job) error {
	log.Println("deploying service", job.Payload["service"], "version", job.Payload["version"])
	time.Sleep(100 * time.Millisecond)
	return fmt.Errorf("deployment failed for %s", job.Payload["service"])
}

func RegisterSampleHandlers(dispatcher *Dispatcher) {
	dispatcher.RegisterHandler(JobType("email"), EmailHandler{})
	dispatcher.RegisterHandler(JobType("deployment"), DeploymentHandler{})
}
