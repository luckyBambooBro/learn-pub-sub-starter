package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
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

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := func (payload T) ([]byte, error) {
		var network bytes.Buffer
		enc := gob.NewEncoder(&network)
		err := enc.Encode(payload)
		if err != nil {
			return nil, err
		}
		return network.Bytes(), nil
	}(val)

	if err != nil {
		fmt.Printf("unable to encode to gob: %v", err)
		return err
	}


	return nil
}