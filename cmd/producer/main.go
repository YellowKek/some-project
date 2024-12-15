package main

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/gorilla/mux"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
)

type Item struct {
	Id       int32   `json:"id"`
	Name     string  `json:"name"`
	Quantity int32   `json:"quantity"`
	Cost     float32 `json:"cost"`
}

type Order struct {
	UserId    uint32  `json:"user_id"`
	Items     []Item  `json:"items"`
	TotalCost float32 `json:"total_cost"`
}

var kafkaBrokers = []string{"kafka:29091"}

const KafkaTopic = "orders"

func main() {
	log.Print("start init")
	producer, err := setupProducer()
	if err != nil {
		panic(err)
	} else {
		log.Println("Kafka AsyncProducer up and running!")
	}
	defer producer.Close()

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	//produceMessages(producer, signals)
	r := mux.NewRouter()
	r.HandleFunc("/order", orderHandler(producer, signals))
	err = http.ListenAndServe(":8080", r)
	if err != nil {
		log.Fatal("", err)
	}
}

func orderHandler(producer sarama.AsyncProducer, signals chan os.Signal) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		err := r.ParseForm()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}(r.Body)
		var order Order
		err = json.Unmarshal(body, &order)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		order.calculateTotal()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		response, err := json.Marshal(order)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, err = w.Write(response)
		if err != nil {
			return
		}
		produceMessages(response, producer, signals)
		log.Print("Produced new message: ", string(response))
	}
}

func (o *Order) calculateTotal() {
	var total float32
	for _, item := range o.Items {
		total += item.Cost
	}
	o.TotalCost = total
}

func setupProducer() (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	return sarama.NewAsyncProducer(kafkaBrokers, config)
}

func produceMessages(message []byte, producer sarama.AsyncProducer, signals chan os.Signal) {
	kafkaMessage := &sarama.ProducerMessage{Topic: KafkaTopic, Value: sarama.ByteEncoder(message)}
	select {
	case producer.Input() <- kafkaMessage:
		log.Println("New Message produced")
	case <-signals:
		producer.AsyncClose()
		return
	}
}
