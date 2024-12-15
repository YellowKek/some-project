package repository

import (
	"consumer/model"
	"context"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type UserRepository interface {
	Create(user model.User) error
	GetById(userId uint32) (model.User, error)
	Update(user model.User) error
}

type UserRepositoryImpl struct {
	collection *mongo.Collection
}

func NewUserRepository(client *mongo.Client) *UserRepositoryImpl {
	return &UserRepositoryImpl{collection: client.Database("test").Collection("user")}
}

func (repo *UserRepositoryImpl) Create(user model.User) error {
	_, err := repo.collection.InsertOne(context.Background(), user)
	return err
}

func (repo *UserRepositoryImpl) GetById(userId uint32) (model.User, error) {
	one := repo.collection.FindOne(context.Background(), bson.M{"_id": userId})
	err := one.Err()
	if err != nil {
		return model.User{}, err
	}

	var res model.User
	err = one.Decode(&res)
	if err != nil {
		return model.User{}, err
	}
	return res, nil
}

func (repo *UserRepositoryImpl) Update(user model.User) error {
	_, err := repo.collection.UpdateOne(context.Background(), bson.M{"_id": user.Id}, bson.M{"$set": user})
	return err
}
