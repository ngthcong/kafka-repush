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

	ReqResInfo struct {
		TimeStamp     time.Time   `json:"timestamp"`
		Level         string      `json:"level"`
		CorrelationID string      `json:"correlationId"`
		Version       string      `json:"version"`
		RequestURL    string      `json:"requestURL"`
		RequestMethod string      `json:"requestMethod"`
		ServiceName   string      `json:"serviceName"`
		FunctionName  string      `json:"functionName"`
		StatusCode    string      `json:"statusCode"`
		ResponseTime  string      `json:"responseTime"`
		Request       interface{} `json:"request"`
		Response      interface{} `json:"response"`
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
		OffsetName   string
		FailPushName string
	}
	offsetInfo struct {
		Offset int64 `json:"offset"`
	}
	ServiceConfig struct {
		FileConfig    FileConfig
		KafkaProducer sarama.SyncProducer
		Topic         string
	}
)

var (
	cronService   *cron.Cron
	serviceConfig ServiceConfig
)

// Key ..
func (r ReqResInfo) Key() string {
	return r.CorrelationID
}

func NewFileConfig() FileConfig {
	return FileConfig{
		LogName:      "log.txt",
		OffsetName:   "lastLine.txt",
		FailPushName: "fail",
	}
}

func NewLogHandler(config ServiceConfig) LogHandler {
	serviceConfig = config
	return &logHandler{}
}

func (handler *logHandler) ReadLog() {
	fmt.Println("Start reading logfile at " + time.Now().String())

	lastLine, err := getLastLine()
	if err != nil {
		fmt.Println(err)
	}

	logFile, err := os.Open(serviceConfig.FileConfig.LogName)
	if err != nil {
		fmt.Println(err)
	}

	scanner := bufio.NewScanner(logFile)

	var newLastLine int64

	for scanner.Scan() {
		newLastLine++
		if newLastLine > lastLine {
			var reqResInfo ReqResInfo
			err := json.Unmarshal(scanner.Bytes(), &reqResInfo)
			if err != nil {
				fmt.Println(err)
			}
			err = handler.Send(serviceConfig.Topic, reqResInfo)
			if err != nil {
				fmt.Println("Fail sending message to kafka server" )
				writeFailPush(scanner.Text())
			}
			fmt.Printf(" Topic: %s || Message: %s \n", serviceConfig.Topic, reqResInfo)
		}
	}

	if scanner.Err() != nil {
		fmt.Printf("Error reading logfile file: %s ", scanner.Err())
	}

	err = storeOffset(newLastLine)
	if err != nil {
		fmt.Printf("Error saving last line: %s ", err)
	}
	return
}

func getLastLine() (int64, error) {
	offsetFile, err := os.Open(serviceConfig.FileConfig.OffsetName)
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
	f, err := os.Create(serviceConfig.FileConfig.OffsetName)
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
		log.Fatal(err)
	}
	if _, err := f.Write([]byte(msg + "\n")); err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
	return nil
}

func (handler *logHandler) Start(cronFormat string) {
	cronService = cron.New()
	cronService.AddFunc(cronFormat, handler.ReadLog)
	cronService.Start()
}

func (*logHandler) Close() {
	//Closing cron and kafka
	cronService.Stop()
	serviceConfig.KafkaProducer.Close()
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
