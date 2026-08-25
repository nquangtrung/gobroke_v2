package main

import (
	"log"
	"os"

	"trontria.com/gobroke/v2/internal/netter"
	"trontria.com/gobroke/v2/internal/server"
	"trontria.com/gobroke/v2/internal/utils"
)

func main() {
	arguments := utils.NamedArguments(os.Args[1:])
	if _, ok := arguments["help"]; ok {
		log.Println("Usage: gobroke [--help]")
		log.Println("Options:")
		log.Println("  --help    		Show this help message")
		log.Println("  --transport    	UNIX or TCP (default: UNIX)")
		log.Println("  --address     	Address for TCP transport (default: localhost:7749)")
		log.Println("  --socket-path  	Path for UNIX socket transport (default: /tmp/gobroke.sock)")
		return
	}

	params := server.BrokerParams{
		Type:       utils.Ternary(arguments["transport"] == "TCP", netter.TCP, netter.UNIX),
		Address:    utils.Ternary(arguments["address"] != "", arguments["address"], "localhost:7749"),
		SocketPath: utils.Ternary(arguments["socket-path"] != "", arguments["socket-path"], "/tmp/gobroke.sock"),
	}

	log.Printf("Starting server with parameters: %+v\n", arguments)
	broker := server.New(params)
	utils.GuardInterrupt(broker)
	go broker.Start()
	log.Println("Server is running. Press Ctrl+C to stop.")

	// Block main goroutine until an interrupt signal is received
	select {}
}
