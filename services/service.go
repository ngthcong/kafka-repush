package services

import (
	"bufio"
	"encoding/json"
	"errors"
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
		//ReadLog(file *os.File,lastLine int64) (int64, error)
		SendMessage(topic string, msg ProducerMessage) error
		StoreLastLine(fileName string, lineNum int64) error
		WriteFailPush(msg string) error
		CloseFile(logFile, failFile *os.File) error
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

func NewLogHandler(producer Producer) *LogHandler {
	return &LogHandler{prod: producer}
}

func (h *LogHandler) GetLog(fileName string) (*os.File, error) {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0444)
	//if err != nil {
	//	return nil,err
	//}
	if _, ok := err.(*os.PathError); ok {
		return nil, errors.New("no such file or directory")
	}
	return file, nil
}

func (h *LogHandler) GetLastLine(fileName string) (int64, error) {
	lastLineFile, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if _, ok := err.(*os.PathError); ok {
		return 0, errors.New("no such file or directory")
	}

	lastLineScanner := bufio.NewScanner(lastLineFile)

	lastLineJson := LastLineInfo{}

	for lastLineScanner.Scan() {
		err = json.Unmarshal(lastLineScanner.Bytes(), &lastLineJson)
		if err != nil {
			return 0, errors.New("unexpected end of JSON input")
		}
	}

	if err = lastLineFile.Close(); err != nil {
		return lastLineJson.LastLine, err
	}

	return lastLineJson.LastLine, nil
}

func (h *LogHandler) GetFailFile(fileName string) (*os.File, error) {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if _, ok := err.(*os.PathError); ok {
		return nil, errors.New("no such file or directory")
	}
	return file, nil
}

func (h *LogHandler) SendMessage(topic string, msg ProducerMessage) error {
	return h.prod.Send(topic, msg)
}

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

func (h *LogHandler) WriteFailPush(file *os.File, msg string) error {
	if _, err := file.Write([]byte(msg + "\n")); err != nil {
		return err
	}
	return nil
}

func (h *LogHandler) CloseFile(logFile, failFile *os.File) error {
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

func (h *LogHandler) Close() error {
	//Closing  kafka
	return h.prod.Close()
}
