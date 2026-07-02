package pubsub

import (
	"encoding/json"
	"fmt"

	"github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp091.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {

	channel, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	ch, err := channel.Consume(queueName, "", false, false, false, false, nil)

	go func() {
		for msg := range ch {
			var body T
			err = json.Unmarshal(msg.Body, &body)
			if err != nil {
				fmt.Printf("error: %v\n", err)
				continue
			}
			handler(body)
			err = msg.Ack(false)
			if err != nil {
				fmt.Printf("error: %v\n", err)
				continue
			}
		}
	}()

	return nil
}
