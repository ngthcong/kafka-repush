package services_test

import (
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"kafka-repush/services"
	"testing"
)

func TestGetLog(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockKafka := NewMockProducer(ctrl)
	service := services.NewLogHandler(mockKafka)
	testCases := []struct {
		name   string
		input  string
		output error
	}{
		{
			name:   "Read file succeed",
			input:  "log.txt",
			output: nil,
		},
		{
			name:   "Read file fail, file does not exist",
			input:  "log",
			output: errors.New("no such file or directory"),
		},
	}
	for _, test := range testCases {
		_, err := service.GetLog(test.input)
		assert.Equal(t, err, test.output)
	}

}

func TestGetFailFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockKafka := NewMockProducer(ctrl)
	service := services.NewLogHandler(mockKafka)
	testCases := []struct {
		name   string
		input  string
		output error
	}{
		{
			name:   "Read file succeed",
			input:  "fail-push.txt",
			output: nil,
		},
	}
	for _, test := range testCases {
		_, err := service.GetFailFile(test.input)
		assert.Equal(t, test.output, err)
	}
}
func TestGetLastLine(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockKafka := NewMockProducer(ctrl)
	service := services.NewLogHandler(mockKafka)
	testCases := []struct {
		name   string
		input  string
		output error
	}{
		{
			name:   "Get last line succeed",
			input:  "test\\last-line1.txt",
			output: nil,
		},
		{
			name:   "Wrong JSON format",
			input:  "test\\last-line2.txt",
			output: errors.New("unexpected end of JSON input"),
		},
		{
			name:   "File empty",
			input:  "test\\last-line3.txt",
			output: nil,
		},
		{
			name:   "File not exist",
			input:  "test\\last-line4.txt",
			output: errors.New("no such file or directory"),
		},
	}
	for _, test := range testCases {
		_, err := service.GetLastLine(test.input)
		assert.Equal(t, test.output, err)
	}
}
func TestSendMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockKafka := NewMockProducer(ctrl)
	service := services.NewLogHandler(mockKafka)

	logInfo := &services.LogInfo{
		Topic:   "test",
		Message: "test",
	}

	failErr := errors.New("sending message failed")

	testCases := []struct {
		name     string
		tearDown func()
		input    *services.LogInfo
		output   error
	}{
		{
			name:  "Send message succeed",
			input: logInfo,
			tearDown: func() {
				mockKafka.EXPECT().Send(logInfo.Topic, logInfo).Times(1).Return(nil)
			},
			output: nil,
		},
		{
			name:  "Send message failed",
			input: logInfo,
			tearDown: func() {
				mockKafka.EXPECT().Send(logInfo.Topic, logInfo).Times(1).Return(failErr)
			},
			output: failErr,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			test.tearDown()
			err := service.SendMessage(test.input.Topic, test.input)
			if err != test.output {
				t.Errorf("got err = %v, expects err = %v", err, test.output)
			}
		})

	}
}
