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

	fmt.Println("Starting Peril client on port 5672")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}
	newGameState := gamelogic.NewGameState(username)

	pubChannel, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix + "." + newGameState.GetUsername(),
		routing.ArmyMovesPrefix+".*",
		pubsub.Transient,
	)

	err = pubsub.SubscribeJSON(
		conn, 
		routing.ExchangePerilDirect, 
		routing.PauseKey + "." + newGameState.GetUsername(), 
		routing.PauseKey, 
		pubsub.Transient,
		handlerPause(newGameState),
	)
	

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix + "." + newGameState.GetUsername(),
		routing.ArmyMovesPrefix + ".*",
		pubsub.Transient,
		handlerMove(pubChannel, newGameState),
	)
	if err != nil {
		log.Fatalf("error subscribing to moves: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix + ".*",
		pubsub.Durable,
		handleWar(pubChannel, newGameState),
	)
	if err != nil {
		log.Fatalf("error subscribing to war: %v", err)
	}

	for {
		userInput := gamelogic.GetInput()
		if len(userInput) == 0 {
			continue
		}

		switch userInput[0]{
		case "spawn":
			err = newGameState.CommandSpawn(userInput)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			move, err := newGameState.CommandMove(userInput)
			if err != nil {
				fmt.Println(err)
				continue
			}

			err = pubsub.PublishJSON(
				pubChannel,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix + "." + newGameState.GetUsername(),
				move,
			)
			if err != nil {
				fmt.Println(err)
				continue
			}

			fmt.Println("Move published successfully")

		case "status":
			newGameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Invalid command")
		}

	}
}