package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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

	//publish message to exchange
	err = pubsub.PublishJSON(ch, 
		routing.ExchangePerilDirect, 
		routing.PauseKey, 
		routing.PlayingState {IsPaused: true},
	)
	if err != nil {
		log.Fatalf("unable to publish playing state: %v", err)
	}

	//wait for control+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("RabbitMQ connection closed...")	
}
