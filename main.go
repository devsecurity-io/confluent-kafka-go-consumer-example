package main

import (
	cmd "confluent-kafka-go-consumer-example/cmd"
	"os"

	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetFormatter(&log.JSONFormatter{})
	// Set default log level; can be overwritten by configuration.
	log.SetLevel(log.InfoLevel)

	err := cmd.Execute()
	if err != nil {
		log.Errorf(err.Error())
		os.Exit(1)
	}
}
