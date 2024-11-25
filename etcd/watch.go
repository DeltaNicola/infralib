package etcd

import (
	"context"
	"encoding/json"

	"github.com/DeltaNicola/infralib/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func WatchKeyChanges(client *clientv3.Client, key string, configChan chan<- interface{}) {
	rch := client.Watch(context.Background(), key)

	logger.Logger.Info(
		"Watcher started",
		zap.String("key", key),
	)

	for wresp := range rch {
		for _, ev := range wresp.Events {
			if ev.Type == clientv3.EventTypePut {
				value := ev.Kv.Value

				logger.Logger.Info(
					"New Configuration Received",
					zap.String("key", key),
					zap.String("value", string(value)),
				)

				var jsonData map[string]interface{}
				if err := json.Unmarshal(value, &jsonData); err != nil {
					logger.Logger.Error(
						"Error Reading New ETCD Configuration",
						zap.String("key", key),
						zap.String("value", string(value)),
					)
					continue
				}

				configChan <- jsonData
				logger.Logger.Info(
					"New ETCD Configuration",
					zap.String("key", key),
					zap.String("value", string(value)),
				)
			}
		}
	}
}
