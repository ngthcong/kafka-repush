package services

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"gopkg.in/robfig/cron.v2"
	"log"
	"os"
	"time"
)

type (
	StringProducerMessage string

	LogInfo struct {
		Topic   string `json:"topic"`
		Message string `json:"message"`
	}

	LogHandler interface {
		Start(cronFormat string)
		ReadLog()
		Close()
		Producer
	}

	logHandler struct{}

	FileConfig struct {
		LogName      string
		LastLineName string
		FailPushName string
	}
	offsetInfo struct {
		Offset int64 `json:"offset"`
	}
	ServiceConfig struct {
		FileConfig    FileConfig
		KafkaProducer sarama.SyncProducer
	}
)

var (
	cronService   *cron.Cron
	serviceConfig ServiceConfig
)

// Key ..
func (r LogInfo) Key() string {
	return r.Topic
}

func NewLogHandler(config ServiceConfig) LogHandler {
	serviceConfig = config
	return &logHandler{}
}

func (handler *logHandler) ReadLog() {
	fmt.Println("Start reading logfile at " + time.Now().String())

	lastLine, err := getLastLine()
	if err != nil {
		log.Fatalln(err)
	}

	logFile, err := os.Open(serviceConfig.FileConfig.LogName)
	if err != nil {
		log.Fatalln(err)
	}

	scanner := bufio.NewScanner(logFile)

	var newLastLine int64

	for scanner.Scan() {
		newLastLine++
		if newLastLine > lastLine {
			var logInfo LogInfo
			err := json.Unmarshal(scanner.Bytes(), &logInfo)
			if err != nil {
				log.Fatalln(err)
			}
			err = handler.Send(logInfo.Topic, logInfo)
			if err != nil {
				fmt.Println("Fail sending message to kafka server")
				err = writeFailPush(scanner.Text())
			}
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("Send success Topic: %s || Message: %s \n", logInfo.Topic, logInfo)
		}
	}

	if scanner.Err() != nil {
		log.Fatalln(scanner.Err())
	}

	err = storeOffset(newLastLine)
	if err != nil {
		log.Fatalln(err)
	}
	return
}

func getLastLine() (int64, error) {
	if !isExist(serviceConfig.FileConfig.LastLineName){
		return  0,nil
	}
	offsetFile, err := os.Open(serviceConfig.FileConfig.LastLineName)
	if err != nil {
		return 0, err
	}

	scanner := bufio.NewScanner(offsetFile)

	offsetJson := offsetInfo{}
	for scanner.Scan() {
		err = json.Unmarshal(scanner.Bytes(), &offsetJson)
		if err != nil {
			return 0, err
		}
	}
	if scanner.Err() != nil {
		return 0, scanner.Err()
	}

	return offsetJson.Offset, nil
}

func storeOffset(lineNum int64) error {
	f, err := os.Create(serviceConfig.FileConfig.LastLineName)
	if err != nil {
		return err
	}

	offset, err := json.Marshal(offsetInfo{Offset: lineNum})
	if err != nil {
		return err
	}

	_, err = f.Write(offset)
	if err != nil {
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}
	return nil
}

func writeFailPush(msg string) error {
	f, err := os.OpenFile(serviceConfig.FileConfig.FailPushName+"_"+time.Now().Format("01-02-2006")+".txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte(msg + "\n")); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}

func (handler *logHandler) Start(cronFormat string) {
	cronService = cron.New()
	_, err := cronService.AddFunc(cronFormat, handler.ReadLog)
	if err != nil {
		log.Fatalln(err)
	}
	cronService.Start()
}

func (*logHandler) Close() {
	//Closing cron and kafka
	cronService.Stop()
	err := serviceConfig.KafkaProducer.Close()
	if err != nil {
		log.Fatalln(err)
	}
}

func (*logHandler) Send(topic string, msg ProducerMessage) error {
	//Sending to kafka server
	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	kafkaMsg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.StringEncoder(jsonMsg),
	}
	_, _, err = serviceConfig.KafkaProducer.SendMessage(kafkaMsg)
	return err
}

func isExist(fileDir string) bool {
	if _, err := os.Stat(fileDir); os.IsNotExist(err) {
		return false
	}
	return true
}
