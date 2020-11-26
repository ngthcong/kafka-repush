package services

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"log"
)

type (
	Producer interface {
		Send(topic string, msg ProducerMessage) error
		Close() error
	}
	KafkaProducer struct {
		prod sarama.SyncProducer
	}

	ProducerMessage interface {
		Key() string
	}
)

func NewProducer(brokers []string) (*KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Timeout.Seconds()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)
	return &KafkaProducer{prod: producer}, err
}

func (k *KafkaProducer) Send(topic string, msg ProducerMessage) error {
	//Sending to kafka server
	jsonMsg, err := json.Marshal(msg.Key())
	if err != nil {
		return err
	}
	kafkaMsg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.StringEncoder(jsonMsg),
	}
	_, _, err = k.prod.SendMessage(kafkaMsg)

	if err == nil {
		log.Printf("Send success Topic: %s || Message: %s \n", topic, msg.Key())
	}
	return err
}

func (k *KafkaProducer) Close() error {
	return k.prod.Close()
}
