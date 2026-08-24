package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"trontria.com/gobroke/v2/internal/client/publisher"
	"trontria.com/gobroke/v2/internal/netter"
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

func publishData(publisher *publisher.Publisher, wg *sync.WaitGroup, topic string, message string) {
	for i := range 5 {
		message := fmt.Sprintf("message:%s:%d", message, i)
		err := publisher.Publish(topic, message)
		if err != nil {
			log.Printf("Failed to publish message: %v", err)
		} else {
			log.Printf("Published message: %s to topic: %s", message, topic)
		}

		time.Sleep(time.Millisecond * 500)
	}
}

func main() {
	publisher := publisher.New(publisher.ProviderParams{
		Type: netter.UNIX,
	})
	guardInterrupt(publisher)
	publisher.Start()
	defer func() {
		log.Println("Cleaning up and stopping publisher...")
		publisher.Stop()
	}()

	var wg sync.WaitGroup
	wg.Go(func() {
		publishData(publisher, &wg, "topic1", "apple")
	})

	wg.Wait()
	log.Println("All publishing tasks completed. Exiting.")
}
