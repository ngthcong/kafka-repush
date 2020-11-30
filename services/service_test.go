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
			input:  "testdata\\log.txt",
			output: nil,
		},
		{
			name:   "Read file fail, file does not exist",
			input:  "log",
			output: services.ErrDirNotFound,
		},
	}
	for _, test := range testCases {
		_, err := service.GetLog(test.input)
		assert.Equal(t, test.output, err)
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
			input:  "testdata\\last-line1.txt",
			output: nil,
		},
		{
			name:   "Wrong JSON format",
			input:  "testdata\\last-line2.txt",
			output: services.ErrJsonInput,
		},
		{
			name:   "File empty",
			input:  "testdata\\last-line3.txt",
			output: nil,
		},
		{
			name:   "File not exist",
			input:  "testdata\\last-line4.txt",
			output: services.ErrDirNotFound,
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

func TestStoreLastLine(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockKafka := NewMockProducer(ctrl)
	service := services.NewLogHandler(mockKafka)
	type lastLineInPut struct {
		fileName string
		lastLine int64
	}
	input := lastLineInPut{
		fileName: "last-line.txt",
		lastLine: 10,
	}
	testCases := []struct {
		name   string
		input  lastLineInPut
		output error
	}{
		{
			name:   "Store last line succeed",
			input:  input,
			output: nil,
		},
	}

	for _, test := range testCases {
		err := service.StoreLastLine(test.input.fileName, test.input.lastLine)
		assert.Equal(t, test.output, err)
	}
}
func TestCloseFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockKafka := NewMockProducer(ctrl)
	service := services.NewLogHandler(mockKafka)

	file, err := ioutil.TempFile("", "example*.txt")
	if err != nil {
		log.Fatal(err)
	}
	testCases := []struct {
		name   string
		input  *os.File
		output error
	}{
		{
			name:   "Close files succeed",
			input:  file,
			output: nil,
		},
		{
			name:   "Missing file",
			input:  nil,
			output: services.ErrInArg,
		},
	}
	for _, test := range testCases {
		err := service.CloseFile(test.input)
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
