package service

import (
	"consumer/model"
	"consumer/repository"
	"encoding/json"
	"github.com/IBM/sarama"
	"log"
	"sync"
)

type MessageService struct {
	mu           sync.Mutex
	db           *repository.OrderRepositoryImpl
	partConsumer sarama.PartitionConsumer
}

func NewMessageService(db *repository.OrderRepositoryImpl) *MessageService {
	return &MessageService{db: db}
}

func (s *MessageService) ProcessMessage(msg *sarama.ConsumerMessage) {
	s.mu.Lock()
	log.Print("Message consumed: ", string(msg.Value))
	var order model.Order
	err := json.Unmarshal(msg.Value, &order)
	if err != nil {
		log.Print(err)
		return
	} else {
		err := s.db.Create(order)
		if err != nil {
			log.Print(err)
		}
	}
	orders, err := s.db.GetAll()
	if err != nil {
		log.Print(err)
		s.mu.Unlock()
		return
	}
	log.Print(orders)
	s.mu.Unlock()
}
