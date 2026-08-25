package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
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
	arguments := utils.NamedArguments(os.Args[1:])
	if _, ok := arguments["help"]; ok {
		log.Println("Usage: gobroke [--help] [topic] [message]")
		log.Println("Options:")
		log.Println("  --help    		Show this help message")
		log.Println("  --topic         	Topic to publish to (default: topic1)")
		log.Println("  --message       	Message to publish (default: apple)")
		log.Println("  --transport      Transport type (UNIX or TCP) (default: UNIX)")
		log.Println("  --address       	Address for TCP transport (default: localhost:7749)")
		log.Println("  --socket-path    Message to publish (default: /tmp/gobroke.sock)")
		log.Println("  --buffer-size    Buffer size for the publisher (default: 100)")
		log.Println("  --timeoutS    	Timeout in seconds for publisher operations (default: 5)")
		log.Println("  --max-retries    Maximum number of retries for failed publish attempts (default: 3)")
		log.Println("  --drop     		DropOldest or DropNewest. Drop policy for the buffer (default: DropNewest)")
		return
	}

	params := publisher.PublisherParams{
		Type:       utils.Ternary(arguments["transport"] == "TCP", netter.TCP, netter.UNIX),
		Address:    utils.Ternary(arguments["address"] != "", arguments["address"], "localhost:7749"),
		SocketPath: utils.Ternary(arguments["socket-path"] != "", arguments["socket-path"], "/tmp/gobroke.sock"),
		BufferSize: utils.Must(strconv.Atoi(utils.Ternary(arguments["buffer-size"] != "", arguments["buffer-size"], "100"))),
		Timeout:    time.Duration(utils.Must(strconv.Atoi(utils.Ternary(arguments["timeoutS"] != "", arguments["timeoutS"], "5")))) * time.Second,
		MaxRetries: utils.Must(strconv.Atoi(utils.Ternary(arguments["max-retries"] != "", arguments["max-retries"], "3"))),
		Drop: utils.Ternary(
			arguments["drop"] != "",
			utils.Ternary(arguments["drop"] == "DropNewest", utils.DropNewest, utils.DropOldest),
			utils.DropNewest,
		),
	}

	log.Printf("Starting publisher with params: %+v", params)
	publisher := publisher.New(params)
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
		publishData(
			publisher,
			utils.Ternary(arguments["topic"] != "", arguments["topic"], "topic1"),
			utils.Ternary(arguments["message"] != "", arguments["message"], "apple"),
		)
		log.Println("Finished publishing to topic1 with apple messages.")
	})

	wg.Wait()
	log.Println("All publishing tasks completed. Exiting.")
}
