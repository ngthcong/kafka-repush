package services

import "github.com/Shopify/sarama"

type(
	Producer interface {
		Send(topic string, msg ProducerMessage) error

	}
	ProducerMessage interface {
		Key() string
	}
)





func NewProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)

	return producer, err
}