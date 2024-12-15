package main

import (
	"consumer/repository"
	"consumer/service"
	"fmt"
	"github.com/IBM/sarama"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"log"
	"os"
)

const KafkaTopic = "orders"

func main() {
	log.Print("consumer init")
	// DB connection
	client, err := mongo.Connect(options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%s@mongo:27017/", os.Getenv("MONGODB_USER"), os.Getenv("MONGODB_PASSWORD"))))
	if err != nil {
		log.Fatal("Error connecting to mongodb", err)
	}
	orderCollection := client.Database("test").Collection("orders")
	orderDB := repository.NewOrderRepositoryImpl(orderCollection)

	consumer, err := sarama.NewConsumer([]string{"kafka:29091"}, nil)
	if err != nil {
		log.Fatalf("Failed to start Sarama consumer: %v", err)

	}
	defer consumer.Close()

	partConsumer, err := consumer.ConsumePartition(KafkaTopic, 0, 0)
	if err != nil {
		log.Fatalf("Failed to start partition consumer: %v", err)
	}
	defer partConsumer.Close()
	messageService := service.NewMessageService(orderDB)

	go ConsumeMessage(messageService, partConsumer)
	select {}
}

func ConsumeMessage(messageService *service.MessageService, partConsumer sarama.PartitionConsumer) {
	for {
		log.Print("start read msg")
		select {
		case msg, ok := <-partConsumer.Messages():
			if !ok {
				log.Print("Channel closed")
				return
			}
			messageService.ProcessMessage(msg)
		}
	}
}
