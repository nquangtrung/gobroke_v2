package main

import (
	"log"

	"trontria.com/gobroke/v2/internal/client/subscriber"
	"trontria.com/gobroke/v2/internal/netter"
	"trontria.com/gobroke/v2/internal/utils"
)

func main() {
	subscriber := subscriber.New(subscriber.SubscriberParams{
		Type:  netter.UNIX,
		Topic: "topic1",
		Handler: func(topic string, message string) {
			log.Printf("Received message on topic %s: %s", topic, message)
		},
	})
	utils.GuardInterrupt(subscriber)
	subscriber.Start()
	for {
	}
}
