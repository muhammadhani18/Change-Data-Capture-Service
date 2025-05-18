package main

import (
	"log"
	"os"
	"fmt"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/muhammadhani18/go-cdc-service/internal/wal"
	"github.com/muhammadhani18/go-cdc-service/internal/kafka"
	"github.com/muhammadhani18/go-cdc-service/internal/store"
	"github.com/muhammadhani18/go-cdc-service/internal/logger"

)

func main() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Config read error: %v\n", err)
	}
	logDBPath := "cdc-logs.db"
    log, err := logger.InitLogger(logDBPath)
    if err != nil {
        fmt.Printf("Logger init error: %v\n", err)
        os.Exit(1)
    }
    defer log.Sync()

	logger_init, _ := zap.NewProduction()
	defer logger_init.Sync()
	logger_init.Info("Starting CDC service...")
	
	// 1) Open or create the BoltDB store
	s, err := store.OpenStore("cdc-checkpoint.db")
	if err != nil {
		logger_init.Fatal("failed to open LSN store", zap.Error(err))
	}
	defer s.Close()

	// 2) Initialize Kafka producer
	producer := kafka.NewProducer()
	defer producer.Close()

	replicator, err := wal.NewReplicator(producer,s)
	if err != nil {
		logger_init.Fatal("Replication setup error", zap.Error(err))
	}

	slot := viper.GetString("postgres.slot_name")
	pub := viper.GetString("postgres.publication_name")

	if err := replicator.StartReplication(slot, pub); err != nil {
		logger_init.Fatal("Replication stream error", zap.Error(err))
	}
}
