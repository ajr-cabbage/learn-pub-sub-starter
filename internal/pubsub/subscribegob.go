package pubsub

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/rabbitmq/amqp091-go"
)

func SubscribeGob[T any](
	conn *amqp091.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {

	channel, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	ch, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range ch {
			var body T
			gobBytes := bytes.NewBuffer(msg.Body)
			decoder := gob.NewDecoder(gobBytes)
			err = decoder.Decode(&body)
			if err != nil {
				fmt.Printf("error: %v\n", err)
			}
			switch ackResult := handler(body); ackResult {
			case Ack:
				err = msg.Ack(false)
				if err != nil {
					fmt.Printf("error: %v\n", err)
					continue
				}
			case NackRequeue:
				err = msg.Nack(false, true)
				if err != nil {
					fmt.Printf("error: %v\n", err)
					continue
				}
			case NackDiscard:
				err = msg.Nack(false, false)
				if err != nil {
					fmt.Printf("error: %v\n", err)
					continue
				}
			}
		}
	}()
	return nil
}
