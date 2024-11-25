package kafka

import (
	"fmt"

	"github.com/DeltaNicola/infralib/logger"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

func toPr(value string) *string { return &value }

func CreateTopic(brokers []string, topic string, partitions int32, replicationFactor int16, retentionMs int64) error {
	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		logger.Logger.Error(
			"Error Creating ClusterAdmin",
			zap.String("topic", topic),
			zap.Strings("brokers", brokers),
			zap.Error(err),
		)
		return fmt.Errorf("errore creazione ClusterAdmin: %v", err)
	}
	defer admin.Close()

	topics, err := admin.ListTopics()
	if err != nil {
		logger.Logger.Error(
			"Error Reading Topics",
			zap.Strings("brokers", brokers),
			zap.Error(err),
		)
		return fmt.Errorf("errore nel recupero dei topic esistenti: %v", err)
	}

	if _, exists := topics[topic]; exists {
		logger.Logger.Info(
			"Topic Already Exists",
			zap.String("topic", topic),
		)
		return nil
	}

	topicDetail := sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
		ConfigEntries: map[string]*string{
			"retention.ms": toPr(fmt.Sprintf("%d", retentionMs)),
		},
	}

	if err := admin.CreateTopic(topic, &topicDetail, false); err != nil {
		logger.Logger.Error(
			"Error Creating Topic",
			zap.String("topic", topic),
			zap.Strings("brokers", brokers),
			zap.Error(err),
		)
		return fmt.Errorf("errore creazione topic: %v", err)
	}

	logger.Logger.Info(
		"Topic Created Successfully",
		zap.String("topic", topic),
		zap.Strings("brokers", brokers),
	)
	return nil
}

func DeleteTopic(brokers []string, topic string) error {
	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		logger.Logger.Error(
			"Error Creating ClusterAdmin",
			zap.String("topic", topic),
			zap.Strings("brokers", brokers),
			zap.Error(err),
		)
		return fmt.Errorf("errore creazione ClusterAdmin: %v", err)
	}
	defer admin.Close()

	topics, err := admin.ListTopics()
	if err != nil {
		logger.Logger.Error(
			"Error Reading Topics",
			zap.Strings("brokers", brokers),
			zap.Error(err),
		)
		return fmt.Errorf("errore nel recupero dei topic esistenti: %v", err)
	}

	if _, exists := topics[topic]; !exists {
		logger.Logger.Info(
			"Topic Already Exists",
			zap.String("topic", topic),
		)
		return nil
	}

	if err := admin.DeleteTopic(topic); err != nil {
		logger.Logger.Error(
			"Error Creating Topic",
			zap.String("topic", topic),
			zap.Strings("brokers", brokers),
			zap.Error(err),
		)
		return fmt.Errorf("errore eliminazione topic: %v", err)
	}

	logger.Logger.Info(
		"Topic Deleted Successfully",
		zap.String("topic", topic),
		zap.Strings("brokers", brokers),
	)
	return nil
}
