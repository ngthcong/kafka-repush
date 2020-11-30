package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"gopkg.in/robfig/cron.v2"
	"kafka-repush/services"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

type (
	serviceConfig struct {
		isDefault        bool
		logFileName      string
		lastLineFileName string
		failPushFileName string
		cronFormat       string
		broker           []string
	}
)

var (
	config serviceConfig
)

func main() {

	defaultCmd := flag.NewFlagSet("default", flag.ExitOnError)
	defLogName := defaultCmd.String("log-name", "", "Log file name")
	defLastLineName := defaultCmd.String("last-line-name", "", "File name to store last readied line")
	defFailName := defaultCmd.String("fail-name", "", "File name to store failed push")
	defBroker := defaultCmd.String("broker", "", "Kafka broker")

	scheduleCmd := flag.NewFlagSet("schedule", flag.ExitOnError)
	slogName := scheduleCmd.String("log-name", "", "Log file name")
	sLastLineName := scheduleCmd.String("last-line-name", "", "File name to store last readied line")
	sFailName := scheduleCmd.String("fail-name", "", "File name to store failed push")
	sBroker := scheduleCmd.String("broker", "", "Kafka broker")
	cronFormat := scheduleCmd.String("cron-format", "", "Cron format")

	if len(os.Args) < 2 {
		fmt.Println("expected 'default' or 'schedule' subcommands")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "default":

		if err := defaultCmd.Parse(os.Args[2:]); err != nil {
			log.Fatalln(err)
		}

		fmt.Println("Service run with default configuration")
		fmt.Println("  Log file's name:", *defLogName)
		fmt.Println("  Last line file's name:", *defLastLineName)
		fmt.Println("  Fail file's name:", *defFailName)
		fmt.Println("  Broker:", *defBroker)
		fmt.Println("")

		config = serviceConfig{
			isDefault:        true,
			logFileName:      *defLogName,
			lastLineFileName: *defLastLineName,
			failPushFileName: *defFailName,
			broker:           strings.Split(*defBroker, " "),
		}

	case "schedule":
		if err := scheduleCmd.Parse(os.Args[2:]); err != nil {
			log.Fatalln(err)
		}

		fmt.Println("Service run with schedule configuration")
		fmt.Println("  Log file's name:", *slogName)
		fmt.Println("  Last line file's name:", *sLastLineName)
		fmt.Println("  Fail file's name:", *sFailName)
		fmt.Println("  Broker:", *sBroker)
		fmt.Println("  Cron format:", *cronFormat)
		fmt.Println("")

		config = serviceConfig{
			isDefault:        false,
			logFileName:      *slogName,
			lastLineFileName: *sLastLineName,
			failPushFileName: *sFailName,
			broker:           strings.Split(*sBroker, " "),
			cronFormat:       *cronFormat,
		}

	default:
		fmt.Println("Expected 'default' or 'schedule' subcommands")
		os.Exit(1)
	}

	startService(config)

}
func startService(config serviceConfig) {

	fmt.Println("---------------Service Running---------------")
	fmt.Println("")

	producer, err := services.NewProducer(config.broker)

	service := services.NewLogHandler(producer)

	// Run with default setting and only run one time
	if config.isDefault {
		logHandlerService(service, config)
		if err := service.Close(); err != nil {
			log.Fatalln(err)
		}
		return
	}

	// Run with schedule setting and run with cron config
	cronService := cron.New()
	_, err = cronService.AddFunc(config.cronFormat, func() {
		logHandlerService(service, config)

	})
	if err != nil {
		log.Fatalln(err)
	}

	cronService.Start()

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGTERM, syscall.SIGINT, os.Interrupt, os.Kill)
	for {
		select {
		case <-exit:
			fmt.Println("Exiting...")
			cronService.Stop()

			if err = service.Close(); err != nil {
				log.Fatalln(err)
			}

			return
		}
	}
}

func logHandlerService(service *services.LogHandler, config serviceConfig) {
	lastLine, err := service.GetLastLine(config.lastLineFileName)

	if err != nil && err.Error() == "no such file or directory" {
		log.Println("Last line file not found, new file created")
	}

	if err != nil && err.Error() == "last line file empty" {
		log.Println(err)
	}
	if err != nil && err.Error() == "unexpected end of JSON input" {
		log.Fatalln("Get last line failed, err: ", err)
	}

	logFile, err := service.GetLog(config.logFileName)
	if err != nil {
		log.Fatalln("Get log failed, err: ", err)
	}

	failPushFile, err := service.GetFailFile(config.failPushFileName)
	if err != nil {
		log.Fatalln("Get fail push file failed, err: ", err)
	}

	log.Println("Start reading log at line: ", lastLine)

	scanner := bufio.NewScanner(logFile)
	var newLastLine int64
	for scanner.Scan() {
		newLastLine++
		if newLastLine > lastLine {
			var logInfo services.LogInfo
			if err := json.Unmarshal(scanner.Bytes(), &logInfo); err != nil {
				log.Println(err)

				if err := service.WriteFailPush(failPushFile, scanner.Text()); err != nil {
					log.Println("Write push failed, err: ", err)
				}
				continue
			}
			if err := service.SendMessage(logInfo.Topic, logInfo); err != nil {
				log.Println("Fail sending message to kafka server")
				if err := service.WriteFailPush(failPushFile, scanner.Text()); err != nil {
					log.Println("Write push failed, err: ", err)
				}
			}
		}
	}

	if err = service.StoreLastLine(config.lastLineFileName, newLastLine); err != nil {
		log.Fatalln("Store last line failed, err: ", err)
	}

	if err = service.CloseFile(logFile); err != nil {
		log.Fatalln(err)
	}
	if err = service.CloseFile(failPushFile); err != nil {
		log.Fatalln(err)
	}

}
