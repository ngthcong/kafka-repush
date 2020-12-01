package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"kafka-repush/services"

	"gopkg.in/robfig/cron.v2"
)

var (
	config                         services.Config
	configFile, logFile, errorFile *os.File
	sugar                          *zap.SugaredLogger
	cronService                    *cron.Cron
)

func main() {
	inputName := flag.String("input", "", "Input file name")
	configName := flag.String("config", "conf.json", "Service configuration")
	errorName := flag.String("error", "error.txt", "File name for storing error push")
	brokers := flag.String("brokers", "", "Kafka brokers(separate by a space)")
	schedule := flag.String("schedule", "", "Schedule run with cron format")

	flag.Parse()

	if *inputName == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	logger := zap.NewExample()
	defer logger.Sync()
	sugar = logger.Sugar()

	producer, err := services.NewProducer(strings.Split(*brokers, " "))
	if err != nil {
		sugar.Infof("Connect to kafka server failed, err: ", err)
	}

	service := services.NewLogHandler(producer)

	if err != nil {
		sugar.Infof("Get configuration failed, err: ", err)
	}

	//Get config with given config flag
	configFile, err = os.OpenFile(*configName, os.O_RDWR, 6440)
	if err != nil {
		sugar.Infof("Get config failed, err: ", err)
		os.Exit(1)
	}

	config, err = service.GetConfig(configFile)
	if err != nil {
		sugar.Infof("Get config failed, err: ", err)
		os.Exit(1)
	}

	//Get logfile with given input flag
	logFile, err = os.OpenFile(*inputName, os.O_RDWR, 0755)
	if err != nil {
		sugar.Infof("Get logfile failed, err: ", err)
		os.Exit(1)
	}

	//Get error file with given error flag
	errorFile, err = os.OpenFile(*errorName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		sugar.Infof("Get error file failed, err: ", err)
		os.Exit(1)
	}

	if *schedule == "" {

		newLastLine := readLogFile(service)
		config.LastLine = newLastLine
		if err := closeService(service); err != nil {
			sugar.Infof("Close service failed, err: ", err)
		}
		os.Exit(0)
	}
	err = runSchedule(service, *schedule)
	if err != nil {
		sugar.Infof("Run Schedule failed, err: ", err)
	}

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGTERM, syscall.SIGINT, os.Interrupt, os.Kill)
	for {
		select {
		case <-exit:
			fmt.Println("Exiting...")
			cronService.Stop()

			if err = closeService(service); err != nil {
				sugar.Infof("Close service with error, err: ", err)
			}
			return
		}
	}

}

func runSchedule(service *services.LogHandler, schedule string) error {

	// Run with schedule setting
	cronService = cron.New()
	_, err := cronService.AddFunc(schedule, func() {
		lastLine := readLogFile(service)
		config.LastLine = lastLine
	})
	if err != nil {
		return err
	}
	cronService.Start()
	return nil
}

func readLogFile(service *services.LogHandler) int64 {
	scanner := bufio.NewScanner(logFile)
	var newLastLine int64
	for scanner.Scan() {
		newLastLine++
		if newLastLine > config.LastLine {
			var logInfo services.LogInfo
			if err := json.Unmarshal(scanner.Bytes(), &logInfo); err != nil {
				sugar.Infof("Unmarshal error: ", err)

				if err := service.WriteFailPush(errorFile, scanner.Text()); err != nil {
					sugar.Infof("Write push failed, err: ", err)
				}
				continue
			}
			if err := service.SendMessage(logInfo.Topic, logInfo); err != nil {
				sugar.Infof("Send message to kafka server failed")
				if err := service.WriteFailPush(errorFile, scanner.Text()); err != nil {
					sugar.Infof("\"Write fail push failed, err: ", err)
				}
			}
		}
	}
	return newLastLine
}

func closeService(service *services.LogHandler) error {
	if err := service.StoreConfig(configFile, config); err != nil {
		return err
	}
	if err := configFile.Close(); err != nil {
		return err
	}
	if err := logFile.Close(); err != nil {
		return err
	}
	if err := errorFile.Close(); err != nil {
		return err
	}
	if err := service.Close(); err != nil {
		return err
	}
	return nil
}
