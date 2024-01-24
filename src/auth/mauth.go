package mauth

import (
	"context"
	"fmt"
	"github.com/892294101/mparallel/src/mconfig"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

var Version = "UNKNOWN"
var BDate = "UNKNOWN"

type Connect struct {
	*mongo.Client
}

type MongoDB struct {
	Source *Connect
	Target *Connect
}

func (m *MongoDB) InitMongo(rule string, url string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	/*
		var ops options.ClientOptions
		ops.SetConnectTimeout() connectTimeoutMS=30
		ops.SetTimeout() timeoutMS=1000
		ops.SetSocketTimeout() socketTimeoutMS=1000
		ops.SetServerSelectionTimeout() serverSelectionTimeoutMS=30000
		ops.SetMaxConnIdleTime() maxIdleTimeMS=10000
	*/
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(fmt.Sprintf("mongodb://%s", url)))
	if err != nil {
		return err
	}

	switch rule {
	case mconfig.SOURCEDB:
		m.Source = &Connect{client}
	case mconfig.TARGETDB:
		m.Target = &Connect{client}
	}

	return client.Ping(ctx, readpref.Primary())
}

func NewMongo() *MongoDB {
	return new(MongoDB)
}
