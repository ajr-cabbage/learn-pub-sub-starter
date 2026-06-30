package pubsub

import (
	"context"
	"encoding/json"

	"github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp091.Channel, exchange, key string, val T) error {

	jsonBytes, err := json.Marshal(val)
	if err != nil {
		return err
	}
	if err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp091.Publishing{ContentType: "application/json", Body: jsonBytes}); err != nil {
		return err
	}

	return nil
}
