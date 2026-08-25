package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
	
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(am gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		mo := gs.HandleMove(am)
		switch mo {
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				ch, 
				routing.ExchangePerilTopic, 
				routing.WarRecognitionsPrefix + "." + gs.GetUsername(),
				gamelogic.RecognitionOfWar{
					Attacker: am.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			fallthrough
		default:
			return pubsub.NackDiscard 
		}
	}
}


func handlerWarMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar)pubsub.Acktype {
	return func(rw gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)

		var msg string

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			fallthrough
		case gamelogic.WarOutcomeYouWon:
			msg = fmt.Sprintf("%s won a war against %s", winner, loser)
		case gamelogic.WarOutcomeDraw:
			msg = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser) 
		default:
			log.Panicln("error - unknown war outcome")
			return pubsub.NackDiscard

		}

		gl := routing.GameLog{
			CurrentTime: time.Now(),
			Message: msg,
			Username: gs.GetUsername(),
		}

		err := PublishGameLog(ch, rw.Attacker.Username, gl)
		if err != nil {
			return pubsub.NackRequeue
		}
		return pubsub.Ack
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