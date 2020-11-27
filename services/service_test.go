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

	logfile1, err := ioutil.TempFile("", "example.*.txt")
	if err != nil {
		log.Fatal(err)
	}
	failFile1, err := ioutil.TempFile("", "example.*.txt")
	if err != nil {
		log.Fatal(err)
	}
	logfile2, err := ioutil.TempFile("", "example.*.txt")
	if err != nil {
		log.Fatal(err)
	}
	failFile2, err := ioutil.TempFile("", "example.*.txt")
	if err != nil {
		log.Fatal(err)
	}

	type inputFile struct {
		logFile      *os.File
		failPushFile *os.File
	}

	input1 := inputFile{
		logFile:      logfile1,
		failPushFile: failFile1,
	}

	input2 := inputFile{
		logFile:      logfile2,
		failPushFile: nil,
	}
	input3 := inputFile{
		logFile:      nil,
		failPushFile: failFile2,
	}
	input4 := inputFile{
		logFile:      nil,
		failPushFile: nil,
	}
	testCases := []struct {
		name   string
		input  inputFile
		output error
	}{
		{
			name:   "Close  files succeed",
			input:  input1,
			output: nil,
		},
		{
			name:   "Missing file",
			input:  input2,
			output: errors.New("invalid argument"),
		},
		{
			name:   "Missing file",
			input:  input3,
			output: errors.New("invalid argument"),
		},
		{
			name:   "Missing both file",
			input:  input4,
			output: errors.New("invalid argument"),
		},
	}
	for _, test := range testCases {
		err := service.CloseFile(test.input.logFile, test.input.failPushFile)
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
	logfile2, err := ioutil.TempFile("", "example.*.txt")
	if err != nil {
		log.Fatal(err)
	}

	type failPush struct {
		logFile *os.File
		msg     string
	}

	input1 := failPush{
		logFile: logfile1,
		msg:     "test string",
	}

	input2 := failPush{
		msg: "test string",
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
			output: errors.New("invalid argument"),
		},
		{
			name:   "Missing message",
			input:  input3,
			output: nil,
		},
		{
			name:   "Missing both file and message",
			input:  input4,
			output: errors.New("invalid argument"),
		},
	}
	for _, test := range testCases {
		err := service.WriteFailPush(test.input.logFile, test.input.msg)
		assert.Equal(t, test.output, err)
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
