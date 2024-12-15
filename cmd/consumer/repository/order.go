package repository

import (
	"consumer/model"
	"context"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type OrderRepository interface {
	Create(order model.Order) error
	GetAll() ([]model.Order, error)
	//GetById(id uint32) (model.Order, error)
}

type OrderRepositoryImpl struct {
	collection *mongo.Collection
}

func NewOrderRepositoryImpl(collection *mongo.Collection) *OrderRepositoryImpl {
	return &OrderRepositoryImpl{collection: collection}
}

func (o *OrderRepositoryImpl) Create(order model.Order) error {
	_, err := o.collection.InsertOne(context.Background(), order)
	return err
}

func (o *OrderRepositoryImpl) GetAll() ([]model.Order, error) {
	find, err := o.collection.Find(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}
	defer find.Close(context.Background())
	var orders []model.Order
	err = find.All(context.Background(), &orders)
	if err != nil {
		return nil, err
	}
	return orders, nil
}
