package services

import (
	"bufio"
	"encoding/json"
	"os"
)

type (
	LogInfo struct {
		Topic   string `json:"topic"`
		Message string `json:"message"`
	}

	LogService interface {
		GetLastLine(fileName string) (int64, error)
		GetLog(fileName string) error
		GetFailFile(fileName string) error
		SendMessage(topic string, msg ProducerMessage) error
		StoreLastLine(fileName string, lineNum int64) error
		WriteFailPush(msg string) error
		CloseFile(file *os.File) error
		Close() error
	}

	LogHandler struct {
		prod Producer
	}

	FileConfig struct {
		LogName      string
		LastLineName string
		FailPushName string
	}

	LastLineInfo struct {
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

//GetLog get log file with given filename
func (h *LogHandler) GetLog(fileName string) (*os.File, error) {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0444)

	if _, ok := err.(*os.PathError); ok {
		return nil, ErrDirNotFound
	}
	return file, nil
}

//GetLastLine get last read line if any
func (h *LogHandler) GetLastLine(fileName string) (int64, error) {
	lastLineFile, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if _, ok := err.(*os.PathError); ok {
		return 0, ErrDirNotFound
	}

	lastLineScanner := bufio.NewScanner(lastLineFile)

	lastLineJson := LastLineInfo{}

	for lastLineScanner.Scan() {
		err = json.Unmarshal(lastLineScanner.Bytes(), &lastLineJson)
		if err != nil {
			return 0, ErrJsonInput
		}
	}

	if err := lastLineFile.Close(); err != nil {
		return lastLineJson.LastLine, err
	}

	return lastLineJson.LastLine, nil
}

//GetFailFile get fail push file, create a new file if there no such file exist
func (h *LogHandler) GetFailFile(fileName string) (*os.File, error) {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	return file, err
}

//SendMessage send message to kafka server
func (h *LogHandler) SendMessage(topic string, msg ProducerMessage) error {
	return h.prod.Send(topic, msg)
}

//StoreLastLine store last read line for next log read, created new file if there no such file
func (h *LogHandler) StoreLastLine(fileName string, lineNum int64) error {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0644)
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

//WriteFailPush write failed push message
func (h *LogHandler) WriteFailPush(file *os.File, msg string) error {
	if _, err := file.Write([]byte(msg + "\n")); err != nil {
		return err
	}
	return nil
}

// CloseFile close log file and fail push file
func (h *LogHandler) CloseFile(file *os.File) error {
	//Closing files
	defer file.Close()
	if err := file.Close(); err != nil {
		return err
	}
	return nil
}

//Close close kafka producer
func (h *LogHandler) Close() error {
	//Closing  kafka
	return h.prod.Close()
}
