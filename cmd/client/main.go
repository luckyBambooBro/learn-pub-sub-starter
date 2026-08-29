package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

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

	ch, err := conn.Channel()
		if err != nil {
			log.Printf("channel connection error: %v", err)
		}

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

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix + "." + gs.GetUsername(),
		routing.ArmyMovesPrefix + ".*",
		pubsub.SimpleQueueTransient,
		handlerMove(gs, ch),
	)
	if err != nil {
		log.Fatalf("could not subscribe to move: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix + ".*",
		pubsub.SimpleQueueDurable,
		handlerWarMove(gs, ch),
	)
	if err != nil {
		log.Fatalf("could not subscribe to war declarations: %v", err)
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
			mv, err := gs.CommandMove(words)
			if err != nil {
				fmt.Printf("invalid move command: %v\n", err)
				continue
			}

			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix + "." + username,
				mv,
			)
			if err != nil {
				log.Printf("err publishing method in client \"move\": %v", err)
				continue
			}
			fmt.Printf("move: %v %v successful\n", words[1], words[2])

		case "status":
			gs.CommandStatus()


		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			if len(words) < 2 {
				fmt.Println("usage: spam <number>")
				continue
			}
			n, err := strconv.Atoi(words[1])
			if err != nil {
				log.Printf("error converting string to int: %v", err)
				continue
			}

			for i := 0; i < n; i++ {
				maliciousLog := gamelogic.GetMaliciousLog()
				err = PublishGameLog(
					ch, 
					gs.GetUsername(), 
					routing.GameLog {
						CurrentTime: time.Now(),
						Message: maliciousLog,
						Username: gs.GetUsername(),
					})
				if err != nil {
					log.Printf("spam command error: %v", err)
				}
			}
			
		case "quit":
			gamelogic.PrintQuit()

		default:
			fmt.Println("unknown command")
		}
	}
}

func PublishGameLog(ch *amqp.Channel, username string, gl routing.GameLog) error {
	return pubsub.PublishGob(
		ch,
		routing.ExchangePerilTopic,
		routing.GameLogSlug + "." + username,
		gl,
	)
}