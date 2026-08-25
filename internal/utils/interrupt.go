package utils

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

type Stopper interface {
	Stop() error
}

func GuardInterrupt(stopper Stopper) {
	// Create a channel to receive OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start a goroutine to handle the signal
	go func() {
		<-sigChan // Wait for a signal
		log.Println("Interrupt received, shutting down...")
		err := stopper.Stop() // Call the provided stop function
		if err != nil {
			log.Printf("Error during shutdown: %v", err)
		}
		os.Exit(0) // Exit the program
	}()
}
