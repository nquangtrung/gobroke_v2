package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"trontria.com/gobroke/v2/internal/client/publisher"
	"trontria.com/gobroke/v2/internal/net"
)

func guardInterrupt(broker *publisher.Publisher) {
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
	provider := publisher.New(publisher.ProviderParams{
		Type: net.UNIX,
	})
	guardInterrupt(provider)
	provider.Start()
	defer provider.Stop()
}
