package main

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func main() {
	// Init config
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("Config read error: %v\n", err)
		os.Exit(1)
	}

	// Init logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	logger.Info("CDC service starting up...")

	// Placeholder for CDC pipeline
	logger.Info("CDC pipeline not implemented yet.")
}