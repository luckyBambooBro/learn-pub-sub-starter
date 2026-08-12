package main

import (
	"fmt"
	"log"

	//"os"
	//"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	const connStr = "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game client connected to RabbitMQ!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("unable to obtain username: %v", err)
	}

	gs := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey + "." + username,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gs),
		)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}


	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			err := gs.CommandSpawn(words)
			if err != nil {
				fmt.Printf("command spawn error: %v\n", err)
				continue
			}
		
		case "move":
			_, err := gs.CommandMove(words)
			if err != nil {
				fmt.Printf("invalid move command: %v\n", err)
				continue
			}
			fmt.Printf("move: %v %v successful\n", words[1], words[2])

		case "status":
			gs.CommandStatus()


		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("Spamming not allowed yet!")

		case "quit":
			gamelogic.PrintQuit()

		default:
			fmt.Println("unknown command")
		}
	}
}
