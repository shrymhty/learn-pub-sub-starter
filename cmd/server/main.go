package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	connection_string := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connection_string)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	fmt.Println("Connection successful. Starting Peril server on 5672")

	msgChannel, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.Durable,
		handlerLogs(),
	)

	if err != nil {
		log.Fatalf("could not starting consuming logs: %v", err)
	}

	gamelogic.PrintServerHelp()
	
	for {
		userInput := gamelogic.GetInput()
		if len(userInput) == 0 {
			continue
		}
		switch userInput[0]{
			case "pause":
				fmt.Println("Sending pause message...")
				err = pubsub.PublishJSON(
					msgChannel,
					routing.ExchangePerilDirect,
					routing.PauseKey,
					routing.PlayingState{
						IsPaused: true,
					},
				)
				if err != nil {
					log.Printf("error publishing pause: %v", err)
				}
			case "resume":
				fmt.Println("Senfing resume message...")
				err = pubsub.PublishJSON(
					msgChannel,
					routing.ExchangePerilDirect,
					routing.PauseKey,
					routing.PlayingState{
						IsPaused: false,
					},
				)
				if err != nil {
					log.Printf("error publishing pause: %v", err)
				}
			case "quit":
				fmt.Println("Exiting...")
				return
			default:
				fmt.Println("Invalid command")
		}
	}
}
