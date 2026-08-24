package main

import (
	"log"
	"time"

	"trontria.com/gobroke/v2/internal/client/subscriber"
	"trontria.com/gobroke/v2/internal/netter"
	"trontria.com/gobroke/v2/internal/utils"
)

func main() {
	subscriber := subscriber.New(subscriber.SubscriberParams{
		Type:      netter.UNIX,
		Topic:     "topic1",
		MaxWorker: 3,
		Handler: func(topic string, message string) {
			time.Sleep(time.Millisecond * 1000) // Simulate processing time
			log.Printf("Handling message on topic %s: %s", topic, message)
		},
	})
	utils.GuardInterrupt(subscriber)
	subscriber.Start()
	for {
	}
}
