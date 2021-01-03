package cmd

import (
	version "confluent-kafka-go-consumer-example/version"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	cobra "github.com/spf13/cobra"
	kafka "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var (
	versionCmd = &cobra.Command{
		Use: "version",
		Run: func(cmd *cobra.Command, args []string) {
			err := showVersion(cmd, args)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		},
	}
)

func showVersion(cmd *cobra.Command, args []string) error {
	log.Tracef("version(...) called")

	fmt.Printf("Version:            %s\n", version.Version)
	fmt.Printf("Built:              %s\n", version.BuildDate)
	fmt.Printf("LibrdkafkaLinkInfo: %s\n", kafka.LibrdkafkaLinkInfo)

	return nil
}
