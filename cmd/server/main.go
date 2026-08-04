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
	fmt.Println("Starting Peril server...")

	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("successfully connected to RabbitMQ!")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open AMQP channel: %v", err)
	}

	gamelogic.PrintServerHelp()
	for {
		inputWords := gamelogic.GetInput()
		if len(inputWords) == 0 {
			continue
		}

		switch inputWords[0] {
		case "pause":
				//publish message to exchange
			err = pubsub.PublishJSON(ch, 
				routing.ExchangePerilDirect, 
				routing.PauseKey, 
				routing.PlayingState {IsPaused: true},
			)
			if err != nil {
				log.Fatalf("unable to publish playing state: %v", err)
			}
			fmt.Println("Pause message sent!")	
		
		case "resume":
			pubsub.PublishJSON(
				ch, 
				routing.ExchangePerilDirect, 
				routing.PauseKey,
				routing.PlayingState {IsPaused: false}, 
			)

		case "quit":
			log.Println("Exiting Peril")
			//unsure if return is the correct way to break out of the loop here. but typing "break" just by itself
			// only breaks out of the switch statement not the outer loop. there is an alternative gemini suggested
			// which is to label the loop (x) and use break followed by x to break out of loop
			return
		
		default:
			log.Println("Unknown user command")
		}
	}



}
