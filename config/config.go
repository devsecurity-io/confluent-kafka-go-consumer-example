package config

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	viper "github.com/spf13/viper"
)

// ParsedConfig holds the configuration settings for this program. The
// ParsedConfig variable is initialized by the Cobra command line parser.
var ParsedConfig Config

// Config ...
type Config struct {
	LogLevel    string                 `mapstructure:"logLevel"`
	Topic       string                 `mapstructure:"topic"`
	KafkaConfig map[string]interface{} `mapstructure:"kafkaConfig"`
}

// Parse ...
func (c *Config) Parse() error {
	log.Tracef("Parse() called")

	var cfgFile string
	cfgFile = viper.GetString("config")

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		viper.SetConfigType("yaml")
	} else {
		// default name for config file is "config.yaml"
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")

		// search directories for config file
		//viper.AddConfigPath("/etc/appname/")
		//viper.AddConfigPath("$HOME/.appname")
		viper.AddConfigPath(".")
	}

	viper.SetDefault("logLevel", "info")

	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil {
		_, ok := err.(viper.ConfigFileNotFoundError)
		if ok {
			log.Warnf(err.Error())
		} else {
			log.Errorf("Error while reading configuration file: %s", err.Error())
		}

		log.Infof("Falling back to default values as no valid configuration file was found")
	}

	err = viper.Unmarshal(c)
	return err
}

func flattenHelper(prefix string, value map[string]interface{}, result map[string]string) {
	for k, v := range value {
		var key string
		if prefix == "" {
			key = k
		} else {
			key = prefix + "." + k
		}

		submap, ok := v.(map[string]interface{})
		if ok {
			flattenHelper(key, submap, result)
		} else {
			// v is not a map, i.e. it is a terminal
			result[key] = fmt.Sprintf("%v", v)
		}
	}
}

func flatten(value map[string]interface{}) map[string]string {
	result := make(map[string]string)
	flattenHelper("", value, result)
	return result
}

// GetKafkaConfigMap ...
func (c *Config) GetKafkaConfigMap() map[string]string {
	return flatten(c.KafkaConfig)
}
