package services_test

import (
	"github.com/stretchr/testify/assert"
	"kafka-repush/services"
	"log"
	"testing"
)

func TestNewProducer(t *testing.T) {
	broker1 := []string{"192.168.75.132:9092"}
	broker2 := []string{"192.168.75.132:9094"}

	testCases := []struct {
		name   string
		input  []string
		output error
	}{
		{
			name:   "Generate new producer succeed",
			input:  broker1,
			output: nil,
		},
		{
			name:   "Generate new producer failed",
			input:  broker2,
			output: services.ErrKafkaNotFound,
		},
	}
	for _, test := range testCases {
		producer, err := services.NewProducer(test.input)
		assert.Equal(t, test.output, err)
		if err == nil {
			if err := producer.Close(); err != nil {
				log.Fatalln(err)
			}
		}
	}
}

func TestSend(t *testing.T) {
	broker1 := []string{"192.168.75.132:9092"}
	producer, err := services.NewProducer(broker1)

	if err != nil {
		log.Fatalln(err)
	}

	logInfo := services.LogInfo{
		Topic:   "Test",
		Message: "Test",
	}
	testCases := []struct {
		name   string
		input  *services.LogInfo
		output error
	}{
		{
			name:   "Send message succeed",
			input:  &logInfo,
			output: nil,
		},
	}
	for _, test := range testCases {
		err := producer.Send(test.input.Topic, test.input)
		assert.Equal(t, test.output, err)
		if err == nil {
			if err := producer.Close(); err != nil {
				log.Fatalln(err)
			}
		}
	}
}
func TestCloseProducer(t *testing.T) {

	broker1 := []string{"192.168.75.132:9092"}
	producer, err := services.NewProducer(broker1)
	if err != nil {
		log.Fatalln(err)
	}

	testCases := []struct {
		name   string
		output error
	}{
		{
			name:   "Close succeed",
			output: nil,
		},
	}
	for _, test := range testCases {
		err := producer.Close()
		assert.Equal(t, test.output, err)
	}
}
