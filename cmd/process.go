package cmd

import (
	config "confluent-kafka-go-consumer-example/config"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	cobra "github.com/spf13/cobra"
	kafka "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var (
	processCmd = &cobra.Command{
		Use: "process",
		Run: func(cmd *cobra.Command, args []string) {
			err := process(cmd, args)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		},
	}
)

// RecordValue represents the struct of the value in a Kafka message
type RecordValue struct {
	Count int
}

func process(cmd *cobra.Command, args []string) error {
	log.Tracef("process(...) called")

	// Create Kafka config map
	kafkaConfigMap := make(kafka.ConfigMap)
	for k, v := range config.ParsedConfig.GetKafkaConfigMap() {
		kafkaConfigMap.SetKey(k, v)
	}

	// Enable the log channel of Confluent Kafka Go library
	kafkaConfigMap.SetKey("go.logs.channel.enable", true)

	// Create consumer instance
	c, err := kafka.NewConsumer(&kafkaConfigMap)
	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	// Go-routine to handle log messages
	go func() {
		for {
			select {
			case logEntry, ok := <-c.Logs():
				if !ok {
					return
				}

				switch logEntry.Level {
				case 0:
					// KERN_EMERG
					log.Panicf("%s|%s|%s", logEntry.Tag, logEntry.Name, logEntry.Message)
				case 1:
					// KERN_ALERT
					log.Fatalf("%s|%s|%s", logEntry.Tag, logEntry.Name, logEntry.Message)
				case 2:
					// KERN_CRIT
					log.Fatalf("%s|%s|%s", logEntry.Tag, logEntry.Name, logEntry.Message)
				case 3:
					// KERN_ERR
					log.Errorf("%s|%s|%s", logEntry.Tag, logEntry.Name, logEntry.Message)
				case 4:
					// KERN_WARNING
					log.Warnf("%s|%s|%s", logEntry.Tag, logEntry.Name, logEntry.Message)
				case 5:
					// KERN_NOTICE
					log.Infof("%s|%s|%s", logEntry.Tag, logEntry.Name, logEntry.Message)
				case 6:
					// KERN_INFO
					log.Infof("%s|%s|%s", logEntry.Tag, logEntry.Name, logEntry.Message)
				case 7:
					// KERN_DEBUG
					log.Debugf("%s|%s|%s", logEntry.Tag, logEntry.Name, logEntry.Message)
				}
			}
		}
	}()

	// Subscribe to topic
	err = c.SubscribeTopics([]string{config.ParsedConfig.Topic}, nil)

	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Process messages
	totalCount := 0
	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			recordKey := string(msg.Key)
			recordValue := msg.Value
			data := RecordValue{}
			err = json.Unmarshal(recordValue, &data)
			if err != nil {
				fmt.Printf("Failed to decode JSON at offset %d: %v", msg.TopicPartition.Offset, err)
				continue
			}
			count := data.Count
			totalCount += count
			fmt.Printf("Consumed record with key %s and value %s, and updated total count to %d\n", recordKey, recordValue, totalCount)
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()

	return nil
}
