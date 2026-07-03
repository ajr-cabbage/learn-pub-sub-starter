package pubsub

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/rabbitmq/amqp091-go"
)

func PublishGameLog(
	ch *amqp091.Channel,
	gs *gamelogic.GameState,
	rw gamelogic.RecognitionOfWar,
	msg string,
) error {

	log := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     msg,
		Username:    gs.GetUsername(),
	}

	err := PublishGob(
		ch,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.GameLogSlug, rw.Attacker.Username),
		log,
	)
	if err != nil {
		return err
	}
	return nil
}
