package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	SimpleQueueTypeTransient SimpleQueueType = "SimpleQueueTypeTransient"
	SimpleQueueTypeDurable SimpleQueueType = "SimpleQueueTypeDurable"

)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		fmt.Printf("unable to marshal %v: %v", val, err)
		return err
	}

	return ch.PublishWithContext(
		context.Background(), 
		exchange, 
		key, 
		false, 
		false, 
		amqp.Publishing {
			ContentType: "application/json",
			Body: data,
		},
	)

}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {

	ch, err := conn.Channel()
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}

	qu, err := ch.QueueDeclare(
		queueName,
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}

	ch.QueueBind(
		queueName,
		key,
		exchange,
		false,
		nil,
	)

	return ch, qu, nil
}