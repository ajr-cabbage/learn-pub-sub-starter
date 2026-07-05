package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	// Init connection and auth user.
	fmt.Println("Starting Peril client...")
	connectionStr := "amqp://guest:guest@localhost:5672"
	conn, err := amqp091.Dial(connectionStr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	fmt.Println("Connection Sucessful!")
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	// init game state
	state := gamelogic.NewGameState(username)
	// pause game message queue
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(state),
	)
	if err != nil {
		log.Fatal(err)
	}
	// army moves message handler
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.Transient,
		handlerMove(state, ch),
	)
	if err != nil {
		log.Fatal(err)
	}
	// war outcome message handler
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"war",
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix),
		pubsub.Durable,
		handlerWar(state, ch),
	)
	if err != nil {
		log.Fatal(err)
	}

EventLoop:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			err = state.CommandSpawn(words)
			if err != nil {
				fmt.Println(err.Error())
			}
		case "move":
			move, err := state.CommandMove(words)
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
				move,
			)
			if err != nil {
				fmt.Println(err.Error())
			} else {
				fmt.Println("Move published sucessfully!")
			}
		case "status":
			state.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(words) < 2 {
				fmt.Println("Invalid syntax. See help.")
				continue
			}
			n, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Println("Invalid input: Not an integer.")
				continue
			}
			for range n {
				spamLog := gamelogic.GetMaliciousLog()
				fakeRW := gamelogic.RecognitionOfWar{
					Attacker: gamelogic.Player{Username: username},
					Defender: gamelogic.Player{},
				}
				err = pubsub.PublishGameLog(
					ch,
					state,
					fakeRW,
					spamLog,
				)
			}
		case "quit":
			fmt.Println("Exiting...")
			break EventLoop
		default:
			fmt.Println("I don't understand the command")
		}
	}
	// block until ctrl-c
	fmt.Println("Running. Press Ctrl+C to Shut Down")
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting down...")
}
