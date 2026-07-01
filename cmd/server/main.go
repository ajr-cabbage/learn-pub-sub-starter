package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connectionStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp091.Dial(connectionStr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	fmt.Println("Connection Successful!")
	gamelogic.PrintServerHelp()
	ch, err := conn.Channel()
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
		case "pause":
			fmt.Println("Sending a Pause Message...")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatal(err)
			}
		case "resume":
			fmt.Println("Sending a Resume Message...")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatal(err)
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
