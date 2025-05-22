package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "strings"
    "time"
    "net/url"

    "github.com/segmentio/kafka-go"
    "github.com/spf13/viper"

    "github.com/muhammadhani18/go-cdc-service/internal/elasticsearch"
)

type Event struct {
    Type      string                 `json:"type"`
    Schema    string                 `json:"schema"`
    Table     string                 `json:"table"`
    Data      map[string]interface{} `json:"data"`
    Timestamp string                 `json:"timestamp"`
    LSN       string                 `json:"lsn"`
}

func main() {
    // Load config
    viper.SetConfigName("config")
    viper.AddConfigPath(".")
    viper.AutomaticEnv()
    if err := viper.ReadInConfig(); err != nil {
        log.Fatalf("config load: %v", err)
    }

    // Elasticsearch client
    esClient, err := es.NewClient()
    if err != nil {
        log.Fatalf("es client init: %v", err)
    }

    // Kafka reader
    r := kafka.NewReader(kafka.ReaderConfig{
        Brokers: viper.GetStringSlice("kafka.brokers"),
        Topic:   viper.GetString("kafka.topic"),
        GroupID: viper.GetString("kafka.groupID"),
    })
    defer r.Close()

    indexPrefix := viper.GetString("elasticsearch.indexPrefix")

    log.Printf("Starting Kafka→Elasticsearch loop…")
    ctx := context.Background()
    
    for {

        m, err := r.FetchMessage(ctx)

        if err != nil {
            log.Printf("fetch message: %v", err)
            time.Sleep(1 * time.Second)
            continue
        }

        var ev Event
        if err := json.Unmarshal(m.Value, &ev); err != nil {
            log.Printf("json unmarshal: %v", err)
            r.CommitMessages(ctx, m)
            continue
        }
        

        // Build index name: e.g. cdc_public_users
        idx := fmt.Sprintf("%s%s_%s", indexPrefix, ev.Schema, ev.Table)
        // Use LSN or primary key as document ID
        docID := url.PathEscape(ev.LSN)
        
        // Prepare body
        body, err := json.Marshal(ev.Data)
        if err != nil {
            log.Printf("marshal body: %v", err)
            r.CommitMessages(ctx, m)
            continue
        }

        
        
        // … after esClient.Index call …
        res, err := esClient.Index(
            idx,
            strings.NewReader(string(body)),
            esClient.Index.WithDocumentID(docID),
            esClient.Index.WithRefresh("true"),
        )
        if err != nil {
            log.Printf("es index error: %v", err)
            } else {
                // 1. Read the entire response
                respBytes, err := io.ReadAll(res.Body)
                // 2. Close it
                res.Body.Close()

                if err != nil {
                    log.Printf("error reading es response: %v", err)
                } else if res.IsError() {
                        // 3. Log the raw response payload
                        log.Printf("es error response: %s", string(respBytes))
                } else {

                log.Printf("Indexed doc id=%s into %s, response: %s", docID, idx, string(respBytes))
            }
        }

    }
}
