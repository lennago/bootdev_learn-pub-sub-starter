package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("couldn't connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game client connected to RabbitMQ!")
	pubCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("couldn't create channel: %v", err)
	}
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("couldn't get username: %v", err)
	}
	gs := gamelogic.NewGameState(username)
	if err := pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, gs.GetUsername()),
		routing.PauseKey, pubsub.SimpleQueueTransient,
		handlerPause(gs),
	); err != nil {
		log.Fatalf("couldn't subscripe to pause: %v", err)
	}
	if err := pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, gs.GetUsername()),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.SimpleQueueTransient,
		handlerMove(gs, pubCh),
	); err != nil {
		log.Fatalf("couldn't subscribe to army moves: %v", err)
	}
	if err := pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix),
		pubsub.SimpleQueueDurable,
		handlerWar(gs, pubCh),
	); err != nil {
		log.Fatalf("couldn't subscripe to war: %v", err)
	}
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "spawn":
			if err := gs.CommandSpawn(input); err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			move, err := gs.CommandMove(input)
			if err != nil {
				fmt.Println(err)
				continue
			}
			if err := pubsub.PublishJSON(
				pubCh,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, move.Player.Username),
				move,
			); err != nil {
				fmt.Printf("error: %s\n", err)
				continue
			}
			fmt.Printf("Moved %v units to %s\n", len(move.Units), move.ToLocation)
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(input) < 2 {
				fmt.Println("usage: spam <n>")
				continue
			}
			n, err := strconv.Atoi(input[1])
			if err != nil {
				fmt.Printf("error: %s is not a valid number\n", input[1])
				continue
			}
			for range n {
				if err := PublishGameLog(
					pubCh,
					username,
					gamelogic.GetMaliciousLog(),
				); err != nil {
					fmt.Printf("error publishing malicious log: %v", err)
				}
			}
			fmt.Printf("Pulished %v malicious logs\n", n)
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			log.Printf("unknown command: %s", input[0])
		}
	}
}

func PublishGameLog(ch *amqp.Channel, username, msg string) error {
	return pubsub.PublishGob(
		ch,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.GameLogSlug, username),
		routing.GameLog{
			Username:    username,
			Message:     msg,
			CurrentTime: time.Now(),
		},
	)
}
