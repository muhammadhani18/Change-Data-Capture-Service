package es

import (
    "github.com/elastic/go-elasticsearch/v8"
    "github.com/spf13/viper"
)

func NewClient() (*elasticsearch.Client, error) {
    cfg := elasticsearch.Config{
        Addresses: viper.GetStringSlice("elasticsearch.addresses"),
        Username:  viper.GetString("elasticsearch.username"),
        Password:  viper.GetString("elasticsearch.password"),
    }
    return elasticsearch.NewClient(cfg)
}
