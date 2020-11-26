package services

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
)

type (
	LogInfo struct {
		Topic   string `json:"topic"`
		Message string `json:"message"`
	}

	LogHandler interface {
		GetLastLine() (int64, error)
		GetLog() error
		GetFailFile() error
		ReadLog(lastLine int64) (int64, error)
		Send(topic string, msg ProducerMessage) error
		StoreLastLine(lineNum int64) error
		WriteFailPush(msg string) error
		CloseFile() error
		Close() error
	}

	logHandler struct {
		ServiceConfig
	}

	FileConfig struct {
		LogName      string
		LastLineName string
		FailPushName string
	}
	LastLineInfo struct {
		LastLine int64 `json:"lastLine"`
	}
	ServiceConfig struct {
		FileConfig    FileConfig
		KafkaProducer sarama.SyncProducer
	}
)

var (
	logFile, failFile *os.File
)

// Key ..
func (r LogInfo) Key() string {
	return r.Message
}

func NewLogHandler(config ServiceConfig) LogHandler {
	return &logHandler{config}
}

func (handler *logHandler) GetLog() error {
	file, err := os.OpenFile(handler.FileConfig.LogName, os.O_RDONLY, 0444)
	if err != nil {
		return err
	}
	logFile = file
	return nil
}

func (handler *logHandler) GetLastLine() (int64, error) {
	if !isExist(handler.FileConfig.LastLineName) {
		return 0, nil
	}
	lastLineFile, err := os.OpenFile(handler.FileConfig.LastLineName, os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}

	lastLineScanner := bufio.NewScanner(lastLineFile)

	lastLineJson := LastLineInfo{}

	for lastLineScanner.Scan() {
		err = json.Unmarshal(lastLineScanner.Bytes(), &lastLineJson)
		if err != nil {
			return 0, err
		}
	}
	defer lastLineFile.Close()

	return lastLineJson.LastLine, lastLineFile.Close()
}

func (handler *logHandler) GetFailFile() error {
	file, err := os.OpenFile(handler.FileConfig.FailPushName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	failFile = file
	return nil
}

func (handler *logHandler) ReadLog(lastLine int64) (int64, error) {
	log.Println("Start reading log at: ", lastLine)
	scanner := bufio.NewScanner(logFile)
	var newLastLine int64
	for scanner.Scan() {
		newLastLine++
		if newLastLine > lastLine {
			var logInfo LogInfo
			if err := json.Unmarshal(scanner.Bytes(), &logInfo); err != nil {
				log.Println(err)

				if err := handler.WriteFailPush(scanner.Text()); err != nil {
					log.Println("Write push failed, err: ", err)
				}
				continue
			}
			if err := handler.Send(logInfo.Topic, logInfo); err != nil {
				log.Println("Fail sending message to kafka server")
				if err := handler.WriteFailPush(scanner.Text()); err != nil {
					log.Println("Write push failed, err: ", err)
				}
			}
		}
	}
	return newLastLine, nil
}

func (handler *logHandler) StoreLastLine(lineNum int64) error {
	file, err := os.OpenFile(handler.FileConfig.LastLineName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	lastLine, err := json.Marshal(LastLineInfo{LastLine: lineNum})
	if err != nil {
		return err
	}
	if err = file.Truncate(0); err != nil {
		return err
	}
	if _, err = file.Write(lastLine); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}

	return nil
}

func (handler *logHandler) WriteFailPush(msg string) error {
	if _, err := failFile.Write([]byte(msg + "\n")); err != nil {
		return err
	}
	return nil
}

func (handler *logHandler) CloseFile() error {
	//Closing files
	defer logFile.Close()
	defer failFile.Close()
	if err := logFile.Close(); err != nil {
		return err
	}
	if err := failFile.Close(); err != nil {
		return err
	}
	return nil
}

func (handler *logHandler) Close() error {
	//Closing  kafka
	return handler.KafkaProducer.Close()
}

func (handler *logHandler) Send(topic string, msg ProducerMessage) error {
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
	_, _, err = handler.KafkaProducer.SendMessage(kafkaMsg)

	if err == nil {
		fmt.Printf("Send success Topic: %s || Message: %s \n", topic, msg.Key())
	}

	return err
}

func isExist(fileDir string) bool {
	if _, err := os.Stat(fileDir); os.IsNotExist(err) {
		return false
	}
	return true
}
