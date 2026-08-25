package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"trontria.com/gobroke/v2/internal/client/publisher"
	"trontria.com/gobroke/v2/internal/netter"
	"trontria.com/gobroke/v2/internal/utils"
)

func publishData(publisher *publisher.Publisher, topic string, message string) {
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
	publisher := publisher.New(publisher.PublisherParams{
		Type: netter.UNIX,
	})
	utils.GuardInterrupt(publisher)
	err := publisher.Start()
	if err != nil {
		log.Fatalf("Failed to start publisher: %v", err)
	}
	defer func() {
		log.Println("Cleaning up and stopping publisher...")
		publisher.Stop()
	}()

	var wg sync.WaitGroup
	wg.Go(func() {
		publishData(publisher, "topic1", "apple")
		log.Println("Finished publishing to topic1 with apple messages.")
	})
	wg.Go(func() {
		publishData(publisher, "topic2", "peach")
		log.Println("Finished publishing to topic2 with peach messages.")
	})
	wg.Go(func() {
		publishData(publisher, "topic1", "banana")
		log.Println("Finished publishing to topic1 with banana messages.")
	})

	wg.Wait()
	log.Println("All publishing tasks completed. Exiting.")
}
