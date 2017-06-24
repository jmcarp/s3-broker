package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/cloudfoundry-community/go-cfclient"
	"github.com/cloudfoundry-community/s3-broker/broker"
)

type Config struct {
	LogLevel     string          `json:"log_level"`
	Username     string          `json:"username"`
	Password     string          `json:"password"`
	S3Config     broker.Config   `json:"s3_config"`
	CloudFoundry cfclient.Config `json:"cloud_foundry`
}

func LoadConfig(configFile string) (config *Config, err error) {
	if configFile == "" {
		return config, errors.New("Must provide a config file")
	}

	file, err := os.Open(configFile)
	if err != nil {
		return config, err
	}
	defer file.Close()

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return config, err
	}

	if err = json.Unmarshal(bytes, &config); err != nil {
		return config, err
	}

	if err = config.Validate(); err != nil {
		return config, fmt.Errorf("Validating config contents: %s", err)
	}

	return config, nil
}

func (c Config) Validate() error {
	if c.LogLevel == "" {
		return errors.New("Must provide a non-empty LogLevel")
	}

	if c.Username == "" {
		return errors.New("Must provide a non-empty Username")
	}

	if c.Password == "" {
		return errors.New("Must provide a non-empty Password")
	}

	if err := c.S3Config.Validate(); err != nil {
		return fmt.Errorf("Validating S3 configuration: %s", err)
	}

	return nil
}
