package main

import (
	"log"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/muhammadhani18/go-cdc-service/internal/wal"
	"github.com/muhammadhani18/go-cdc-service/internal/kafka"
	"github.com/muhammadhani18/go-cdc-service/internal/store"
)

func main() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Config read error: %v\n", err)
	}

	logger, _ := zap.NewProduction()
	defer logger.Sync()
	logger.Info("Starting CDC service...")
	
	// 1) Open or create the BoltDB store
	s, err := store.OpenStore("cdc-checkpoint.db")
	if err != nil {
		logger.Fatal("failed to open LSN store", zap.Error(err))
	}
	defer s.Close()

	// 2) Initialize Kafka producer
	producer := kafka.NewProducer()
	defer producer.Close()

	replicator, err := wal.NewReplicator(producer,s)
	if err != nil {
		logger.Fatal("Replication setup error", zap.Error(err))
	}

	slot := viper.GetString("postgres.slot_name")
	pub := viper.GetString("postgres.publication_name")

	if err := replicator.StartReplication(slot, pub); err != nil {
		logger.Fatal("Replication stream error", zap.Error(err))
	}
}
