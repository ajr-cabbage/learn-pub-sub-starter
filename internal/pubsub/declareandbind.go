package pubsub

import (
	"github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func DeclareAndBind(
	conn *amqp091.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp091.Channel, amqp091.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp091.Queue{}, err
	}
	var isDurable bool
	if queueType == Durable {
		isDurable = true
	} else {
		isDurable = false
	}
	table := amqp091.Table{"x-dead-letter-exchange": "peril_dlx"}
	qu, err := ch.QueueDeclare(queueName, isDurable, !isDurable, !isDurable, false, table)
	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp091.Queue{}, err
	}

	return ch, qu, nil
}
