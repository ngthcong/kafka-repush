package services_test

import (
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"kafka-repush/services"
	"log"
	"os"
	"testing"
)

func TestGetConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockKafka := NewMockProducer(ctrl)
	service := services.NewLogHandler(mockKafka)

	//Get config with given config flag
	configFile1, err := os.OpenFile("testdata\\conf.json", os.O_RDWR, 6440)
	if err != nil {
		log.Fatalln("Get config failed, err: ", err)
	}
	//Get config with given config flag
	configFile2, err := os.OpenFile("testdata\\conf2.json", os.O_RDWR, 6440)
	if err != nil {
		log.Fatalln("Get config failed, err: ", err)
	}

	testCases := []struct {
		name   string
		input  *os.File
		output error
	}{
		{
			name:   "Get last line succeed",
			input:  configFile1,
			output: nil,
		},
		{
			name:   "Wrong JSON format",
			input:  configFile2,
			output: services.ErrJsonInput,
		},
	}
	for _, test := range testCases {
		_, err := service.GetConfig(test.input)
		assert.Equal(t, test.output, err)
	}
}

func TestSendMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockKafka := NewMockProducer(ctrl)
	service := services.NewLogHandler(mockKafka)

	logInfo := &services.LogInfo{
		Topic:   "testdata",
		Message: "testdata",
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

func TestStoreConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockKafka := NewMockProducer(ctrl)
	service := services.NewLogHandler(mockKafka)
	configFile1, err := os.OpenFile("testdata\\conf.json", os.O_RDWR, 6440)
	if err != nil {
		log.Fatalln("Get config failed, err: ", err)
	}
	mockConfig := services.Config{LastLine: 0}
	type configInput struct {
		file   *os.File
		config services.Config
	}
	input := configInput{
		file:   configFile1,
		config: mockConfig,
	}
	testCases := []struct {
		name   string
		input  configInput
		output error
	}{
		{
			name:   "Store last line succeed",
			input:  input,
			output: nil,
		},
	}

	for _, test := range testCases {
		err := service.StoreConfig(test.input.file, test.input.config)
		assert.Equal(t, test.output, err)
	}
}

func TestWriteFailPush(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockKafka := NewMockProducer(ctrl)
	service := services.NewLogHandler(mockKafka)

	logfile1, err := ioutil.TempFile("", "example.*.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer logfile1.Close()

	logfile2, err := ioutil.TempFile("", "example.*.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer logfile2.Close()

	type failPush struct {
		logFile *os.File
		msg     string
	}

	input1 := failPush{
		logFile: logfile1,
		msg:     "testdata string",
	}

	input2 := failPush{
		msg: "testdata string",
	}
	input3 := failPush{
		logFile: logfile2,
	}
	input4 := failPush{}

	testCases := []struct {
		name   string
		input  failPush
		output error
	}{
		{
			name:   "Write succeed",
			input:  input1,
			output: nil,
		},
		{
			name:   "Missing file",
			input:  input2,
			output: services.ErrInArg,
		},
		{
			name:   "Missing message",
			input:  input3,
			output: nil,
		},
		{
			name:   "Missing both file and message",
			input:  input4,
			output: services.ErrInArg,
		},
	}
	for _, test := range testCases {
		err := service.WriteFailPush(test.input.logFile, test.input.msg)
		assert.Equal(t, test.output, err)
	}

	if err = logfile1.Close(); err != nil {
		log.Fatalln(err)
	}
	if err = logfile2.Close(); err != nil {
		log.Fatalln(err)
	}
}
func TestClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockKafka := NewMockProducer(ctrl)
	service := services.NewLogHandler(mockKafka)

	failErr := errors.New("error closing kafka producer")

	testCases := []struct {
		name     string
		tearDown func()
		output   error
	}{
		{
			name: "Close producer succeed",
			tearDown: func() {
				mockKafka.EXPECT().Close().Times(1).Return(nil)
			},
			output: nil,
		},
		{
			name: "Close producer failed",
			tearDown: func() {
				mockKafka.EXPECT().Close().Times(1).Return(failErr)
			},
			output: failErr,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			test.tearDown()
			err := service.Close()
			if err != test.output {
				t.Errorf("got err = %v, expects err = %v", err, test.output)
			}
		})
	}
}
