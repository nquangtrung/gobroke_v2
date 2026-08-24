package main

import (
	"trontria.com/gobroke/v2/internal/netter"
	"trontria.com/gobroke/v2/internal/server"
	"trontria.com/gobroke/v2/internal/utils"
)

func main() {
	broker := server.New(server.BrokerParams{
		Type: netter.UNIX,
	})
	utils.GuardInterrupt(broker)
	broker.Start()
	defer broker.Stop()
}
