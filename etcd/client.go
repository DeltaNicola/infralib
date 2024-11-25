package etcd

import (
	"sync"
	"time"

	"github.com/DeltaNicola/infralib/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	etcdClient *clientv3.Client
	once       sync.Once
)

type InitEndpoint struct {
	Init bool `yaml:"init"`
}

func NewEtcdClient(endpoints []string) {
	once.Do(func() {
		var err error

		config := clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: 5 * time.Second,
		}

		etcdClient, err = clientv3.New(config)
		if err != nil {
			logger.Logger.Fatal(
				"Error Connecting To ETCD",
				zap.Error(err),
			)
		}
	})
}

func GetEtcdClient() *clientv3.Client {
	return etcdClient
}

func CloseEtcdClient() {
	if etcdClient != nil {

		err := etcdClient.Close()
		if err != nil {
			logger.Logger.Error(
				"Error Closing ETCD Client",
				zap.Error(err),
			)
			return
		}

		logger.Logger.Info(
			"ETCD Client Closed Successfully",
		)
	}
}
