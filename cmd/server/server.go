package main

import (
	"log"

	"trontria.com/gobroke/v2/internal/netter"
	"trontria.com/gobroke/v2/internal/server"
	"trontria.com/gobroke/v2/internal/utils"
)

func main() {
	broker := server.New(server.BrokerParams{
		Type: netter.UNIX,
	})
	utils.GuardInterrupt(broker)
	go broker.Start()
	log.Println("Server is running. Press Ctrl+C to stop.")

	// Block main goroutine until an interrupt signal is received
	select {}
}
