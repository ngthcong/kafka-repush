package main

import (
	"fmt"
	"kafka-repush/services"
	"log"
	"os"
	"os/signal"
	"syscall"
)
var(
	brokers = []string{"192.168.75.132:9092"}

)

func main() {
	fmt.Println("---------------Starting service---------------")
	producer,err := services.NewProducer(brokers)
	if err != nil{
		log.Fatal(err)
	}
	serviceConfig := services.ServiceConfig{
		FileConfig:    services.NewFileConfig(),
		KafkaProducer: producer,
		Topic: "Bogie",
	}
	service := services.NewLogHandler(serviceConfig)
	// 5 4 * * *  -- At 04:05
	service.Start("@every 0h0m15s")
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGTERM, syscall.SIGINT, os.Interrupt, os.Kill)
	for {
		select {
		case <-exit:
			service.Close()
			fmt.Println("Exiting...")
			return
		}
	}
}


