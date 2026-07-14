package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQQueue struct {
	connection     *amqp.Connection
	publishChannel *amqp.Channel
	consumeChannel *amqp.Channel
	queueName      string
	deliveries     <-chan amqp.Delivery
	consumerOnce   sync.Once
	consumerErr    error
	publishMu      sync.Mutex
	ackMu          sync.Mutex
}

func NewRabbitMQQueue(rabbitURL string, queueName string, prefetch int) (*RabbitMQQueue, error) {
	connection, err := amqp.Dial(rabbitURL)
	if err != nil {
		return nil, err
	}

	publishChannel, err := connection.Channel()
	if err != nil {
		connection.Close()
		return nil, err
	}

	consumeChannel, err := connection.Channel()
	if err != nil {
		publishChannel.Close()
		connection.Close()
		return nil, err
	}

	_, err = publishChannel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		amqp.Table{"x-max-priority": int32(3)},
	)
	if err != nil {
		consumeChannel.Close()
		publishChannel.Close()
		connection.Close()
		return nil, err
	}

	if err := publishChannel.Confirm(false); err != nil {
		consumeChannel.Close()
		publishChannel.Close()
		connection.Close()
		return nil, err
	}

	if err := consumeChannel.Qos(prefetch, 0, false); err != nil {
		consumeChannel.Close()
		publishChannel.Close()
		connection.Close()
		return nil, err
	}

	return &RabbitMQQueue{
		connection:     connection,
		publishChannel: publishChannel,
		consumeChannel: consumeChannel,
		queueName:      queueName,
	}, nil
}

func (q *RabbitMQQueue) Enqueue(ctx context.Context, job Job) error {
	body, err := json.Marshal(struct {
		JobID int `json:"job_id"`
	}{JobID: job.ID})
	if err != nil {
		return err
	}

	q.publishMu.Lock()
	defer q.publishMu.Unlock()

	confirmation, err := q.publishChannel.PublishWithDeferredConfirmWithContext(
		ctx,
		"",
		q.queueName,
		true,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Priority:     rabbitPriority(job.Priority),
			Body:         body,
		},
	)
	if err != nil {
		return err
	}

	acknowledged, err := confirmation.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acknowledged {
		return errors.New("rabbitmq did not confirm the job")
	}

	return nil
}

func (q *RabbitMQQueue) NextJob(ctx context.Context) (JobDelivery, error) {
	q.startConsumer()
	if q.consumerErr != nil {
		return JobDelivery{}, q.consumerErr
	}

	select {
	case delivery, open := <-q.deliveries:
		if !open {
			return JobDelivery{}, errors.New("rabbitmq delivery channel closed")
		}

		var message struct {
			JobID int `json:"job_id"`
		}
		if err := json.Unmarshal(delivery.Body, &message); err != nil {
			_ = delivery.Nack(false, false)
			return JobDelivery{}, fmt.Errorf("decode rabbitmq job: %w", err)
		}

		return JobDelivery{
			JobID: message.JobID,
			ack: func() error {
				q.ackMu.Lock()
				defer q.ackMu.Unlock()
				return delivery.Ack(false)
			},
			nack: func() error {
				q.ackMu.Lock()
				defer q.ackMu.Unlock()
				return delivery.Nack(false, true)
			},
		}, nil
	case <-ctx.Done():
		return JobDelivery{}, ctx.Err()
	}
}

func (q *RabbitMQQueue) startConsumer() {
	q.consumerOnce.Do(func() {
		q.deliveries, q.consumerErr = q.consumeChannel.Consume(
			q.queueName,
			"",
			false,
			false,
			false,
			false,
			nil,
		)
	})
}

func (q *RabbitMQQueue) Close() error {
	return q.connection.Close()
}

func rabbitPriority(priority JobPriority) uint8 {
	switch priority {
	case JobPriorityHigh:
		return 3
	case JobPriorityLow:
		return 1
	default:
		return 2
	}
}
