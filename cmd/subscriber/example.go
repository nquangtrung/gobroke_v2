package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"trontria.com/gobroke/v2/internal/client/subscriber"
	"trontria.com/gobroke/v2/internal/netter"
	"trontria.com/gobroke/v2/internal/utils"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	arguments := utils.NamedArguments(os.Args[1:])
	if _, ok := arguments["help"]; ok {
		log.Println("Usage: gobroke [--help] [topic] [message]")
		log.Println("Options:")
		log.Println("  --help    		Show this help message")
		log.Println("  --topic         	Topic to publish to (default: topic1)")
		log.Println("  --transport      Transport type (UNIX or TCP) (default: UNIX)")
		log.Println("  --address       	Address for TCP transport (default: localhost:7749)")
		log.Println("  --socket-path    Message to publish (default: /tmp/gobroke.sock)")
		log.Println("  --max-worker		Maximum number of concurrent workers (default: 3)")
		return
	}

	subscriber := subscriber.New(subscriber.SubscriberParams{
		Type:       utils.Ternary(arguments["transport"] == "TCP", netter.TCP, netter.UNIX),
		Topic:      utils.Ternary(arguments["topic"] != "", arguments["topic"], "topic1"),
		Address:    utils.Ternary(arguments["address"] != "", arguments["address"], "localhost:7749"),
		SocketPath: utils.Ternary(arguments["socket-path"] != "", arguments["socket-path"], "/tmp/gobroke.sock"),
		MaxWorker:  utils.Must(strconv.Atoi(utils.Ternary(arguments["max-worker"] != "", arguments["max-worker"], "3"))),
		Handler: func(topic string, message string) {
			time.Sleep(time.Millisecond * 1000) // Simulate processing time
			log.Printf("Handling message on topic %s: %s", topic, message)
		},
	})
	err := subscriber.Start(ctx)
	if err != nil {
		log.Fatalf("Failed to start subscriber: %v", err)
	}

	// Keep the main function running
	subscriber.Wait()
	log.Println("Subscriber stopped gracefully.")
}
