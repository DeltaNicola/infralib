package kafka

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/DeltaNicola/infralib/logger"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type KafkaProducer struct {
	producer sarama.SyncProducer
}

func NewKafkaProducer(brokers []string) (*KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 10 * time.Second

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		logger.Logger.Error(
			"Error Instantiating Producer",
			zap.Strings("brokers", brokers),
			zap.Error(err),
		)
		return nil, fmt.Errorf("errore nella creazione del produttore Kafka: %v", err)
	}

	logger.Logger.Info(
		"Producer Created Successfully",
		zap.Strings("brokers", brokers),
	)

	return &KafkaProducer{producer: producer}, nil
}

func (kp *KafkaProducer) PushToTopic(topic string, message interface{}) error {
	data, err := json.Marshal(message)
	if err != nil {
		logger.Logger.Error(
			"Error JSON Conversion",
			zap.String("topic", topic),
			zap.Error(err),
		)
		return fmt.Errorf("errore nella conversione in JSON del messaggio: %v", err)
	}

	kafkaMessage := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}

	partition, offset, err := kp.producer.SendMessage(kafkaMessage)
	if err != nil {
		logger.Logger.Error(
			"Error Sending to Topic",

			zap.Error(err),
		)
		return fmt.Errorf("errore durante l'invio al topic Kafka: %v", err)
	}

	logger.Logger.Info(
		"Messaged Posted Successfully",
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
	)

	return nil
}

func (kp *KafkaProducer) Close() error {
	return kp.producer.Close()
}
