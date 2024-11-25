package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/DeltaNicola/infralib/logger"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type KafkaConsumer struct {
	consumer sarama.Consumer
}

func NewKafkaConsumer(brokers []string) (*KafkaConsumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		logger.Logger.Error(
			"Error Instantiating Consumer",
			zap.Strings("brokers", brokers),
			zap.Error(err),
		)
		return nil, fmt.Errorf("errore nella creazione del consumatore Kafka: %v", err)
	}

	logger.Logger.Info(
		"Consumer Created Successfully",
		zap.Strings("brokers", brokers),
	)

	return &KafkaConsumer{consumer: consumer}, nil
}

func (kc *KafkaConsumer) ReadFromTopic(ctx context.Context, topic string, messageHandler func([]byte)) error {
	partitions, err := kc.consumer.Partitions(topic)
	if err != nil {
		logger.Logger.Error(
			"Error Consuming Partition",
			zap.String("topic", topic),
			zap.Error(err),
		)
		return fmt.Errorf("errore durante l'ottenimento delle partizioni del topic: %v", err)
	}

	var wg sync.WaitGroup

	for _, partition := range partitions {
		pc, err := kc.consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			logger.Logger.Error(
				"Error Consuming Partition",
				zap.String("topic", topic),
				zap.Int32("partition", partition),
				zap.Error(err),
			)
			return fmt.Errorf("errore durante il consumo della partizione %d del topic %s: %v", partition, topic, err)
		}

		wg.Add(1)

		go func(pc sarama.PartitionConsumer, partition int32) {
			defer func() {
				pc.Close()
				wg.Done()
			}()

			for {
				select {
				case msg := <-pc.Messages():

					logger.Logger.Info(
						"New Message Received",
						zap.String("topic", topic),
						zap.Int32("partition", msg.Partition),
					)

					messageHandler(msg.Value)
				case err := <-pc.Errors():
					logger.Logger.Error(
						"Consumer Error",
						zap.String("topic", topic),
						zap.Int32("partition", err.Partition),
						zap.Error(err),
					)
				case <-ctx.Done():
					logger.Logger.Warn(
						"Interrupting Consumer",
						zap.String("topic", topic),
					)
					return
				}
			}
		}(pc, partition)
	}

	<-ctx.Done()
	logger.Logger.Warn(
		"Context Canceled: Waiting for Partition Consumers to Stop",
		zap.String("topic", topic),
	)

	wg.Wait()

	logger.Logger.Info(
		"Partition Consumer Stopped",
		zap.String("topic", topic),
	)
	return kc.consumer.Close()
}

func (kc *KafkaConsumer) Close() error {
	return kc.consumer.Close()
}
