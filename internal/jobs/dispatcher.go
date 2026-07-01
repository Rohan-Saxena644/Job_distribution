package jobs

import (
	"errors"
	"fmt"
)

var (
	ErrHandlerNotFound = errors.New("handler not found")
)

type HandlerFunc func(Job) error

type Dispatcher struct {
	handlers map[JobType]HandlerFunc
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		handlers: make(map[JobType]HandlerFunc),
	}
}

func (d *Dispatcher) Register(jobType JobType, handler HandlerFunc) {
	d.handlers[jobType] = handler
}

func (d *Dispatcher) Run(job Job) error {
	handler, exists := d.handlers[job.Type]
	if !exists {
		return fmt.Errorf("%w: %s", ErrHandlerNotFound, job.Type)
	}

	return handler(job)
}
