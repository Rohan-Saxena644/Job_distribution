package jobs

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrHandlerNotFound = errors.New("handler not found")
)

type JobHandler interface {
	Execute(context.Context, Job) error
}

type HandlerFunc func(context.Context, Job) error

func (fn HandlerFunc) Execute(ctx context.Context, job Job) error {
	return fn(ctx, job)
}

type Dispatcher struct {
	handlers map[JobType]JobHandler
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		handlers: make(map[JobType]JobHandler),
	}
}

func (d *Dispatcher) RegisterHandler(jobType JobType, handler JobHandler) {
	d.handlers[jobType] = handler
}

func (d *Dispatcher) Run(ctx context.Context, job Job) error {
	handler, exists := d.handlers[job.Type]
	if !exists {
		return fmt.Errorf("%w: %s", ErrHandlerNotFound, job.Type)
	}

	return handler.Execute(ctx, job)
}
