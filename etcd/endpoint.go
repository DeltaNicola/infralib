package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/DeltaNicola/infralib/logger"
	"go.uber.org/zap"
)

func CreateOrUpdateEndpoint(key string, value interface{}) error {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		logger.Logger.Error(
			"Error JSON Conversion",
			zap.String("key", key),
			zap.Reflect("value", value),
			zap.Error(err),
		)
	}

	cli := GetEtcdClient()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = cli.Put(ctx, key, string(jsonValue))
	if err != nil {
		logger.Logger.Error(
			"Error Updating ETCD",
			zap.Error(err),
		)

		return fmt.Errorf("error updating ETCD %s: %v", key, err)
	}
	logger.Logger.Error(
		"ETCD Updated Successfully",
		zap.String("key", key),
		zap.Reflect("value", value),
	)

	return nil
}

func GetEndpoint(key string) (interface{}, error) {
	client := GetEtcdClient()

	resp, err := client.Get(context.Background(), key)
	if err != nil {
		return "", fmt.Errorf("errore lettura stato ordine: %v", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	value := string(resp.Kvs[0].Value)

	var jsonData map[string]interface{}
	if err := json.Unmarshal([]byte(value), &jsonData); err == nil {
		return jsonData, nil
	}

	return value, nil
}

func DeleteEndpoint(key string) error {
	cli := GetEtcdClient()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := cli.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("errore eliminazione key %s: %v", key, err)
	}
	log.Printf("key %s eliminato con successo", key)
	return nil
}
