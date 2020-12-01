package services

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type (
	LogInfo struct {
		Topic   string `json:"topic"`
		Message string `json:"message"`
	}

	LogService interface {
		GetConfig(fileName string) (Config, error)
		SendMessage(topic string, msg ProducerMessage) error
		StoreConfig(file *os.File, config Config) error
		WriteFailPush(msg string) error
		Close() error
	}

	LogHandler struct {
		prod Producer
	}

	Config struct {
		LastLine int64 `json:"lastLine"`
	}
)

// Key ..
func (r LogInfo) Key() string {
	return r.Message
}

//NewLogHandler create new LogHandler
func NewLogHandler(producer Producer) *LogHandler {
	return &LogHandler{prod: producer}
}

//GetLastLine get last read line if any
func (h *LogHandler) GetConfig(file *os.File) (Config, error) {

	configByte, err := ioutil.ReadAll(file)
	if err != nil {
		return Config{}, err
	}
	config := Config{}
	err = json.Unmarshal(configByte, &config)
	if err != nil {
		return Config{}, ErrJsonInput
	}
	return config, nil
}

//SendMessage send message to kafka server
func (h *LogHandler) SendMessage(topic string, msg ProducerMessage) error {
	return h.prod.Send(topic, msg)
}

//StoreLastLine store last read line for next log read, created new file if there no such file
func (h *LogHandler) StoreConfig(file *os.File, config Config) error {
	configByte, err := json.Marshal(config)
	if err != nil {
		return err
	}
	if err := file.Truncate(0); err != nil {
		return err
	}
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}
	if _, err = file.Write(configByte); err != nil {
		return err
	}
	return nil
}

//WriteFailPush write failed push message
func (h *LogHandler) WriteFailPush(file *os.File, msg string) error {
	if _, err := file.Write([]byte(msg + "\n")); err != nil {
		return err
	}
	return nil
}

//Close close kafka producer
func (h *LogHandler) Close() error {
	//Closing  kafka
	return h.prod.Close()
}
