package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"

	"github.com/rabbitmq/amqp091-go"
)

func PublishGob[T any](ch *amqp091.Channel, exchange, key string, val T) error {
	var gobBytes bytes.Buffer
	encoder := gob.NewEncoder(&gobBytes)
	err := encoder.Encode(val)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp091.Publishing{ContentType: "application/gob", Body: gobBytes.Bytes()},
	)
	if err != nil {
		return err
	}
	return nil
}
