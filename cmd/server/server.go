package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"trontria.com/gobroke/v2/internal/netter"
	"trontria.com/gobroke/v2/internal/server"
)

func guardInterrupt(broker *server.Broker) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down gracefully...")
		broker.Stop() // This deletes the socket file
		os.Exit(0)
	}()
}

func main() {
	broker := server.New(server.BrokerParams{
		Type: netter.UNIX,
	})
	guardInterrupt(broker)
	broker.Start()
	defer broker.Stop()
}
