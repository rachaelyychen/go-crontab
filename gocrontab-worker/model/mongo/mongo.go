package mongo

import (
	"context"
	"fmt"
	"github.com/lexkong/log"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gocrontab-worker/utils"
	"time"
)

/**
* @project: gocrontab-worker
*
* @description:
*
* @author: cyy
*
* @create: 9/11/19 2:20 PM
**/

const (
	DefaultMongoUri            = "mongodb://localhost:27017"
	DefaultMongoConnectTimeout = 5000
)

var (
	mongoLogStr = fmt.Sprintf(utils.LOGSTR, "crontab-worker", "mongo")
	client      *mongo.Client
	config      *mongoConfig
	ctx         context.Context
	opts        *options.ClientOptions
	err         error
	MongoMgr    *MongoManager
)

type mongoConfig struct {
	MongoUri            string
	MongoConnectTimeout int
}

type MongoManager struct {
	Client *mongo.Client
}

func Init() {
	config = &mongoConfig{}
	initConfig()

	ctx, _ = context.WithTimeout(context.Background(), time.Duration(config.MongoConnectTimeout)*time.Millisecond)
	opts = options.Client().ApplyURI(config.MongoUri)
	if client, err = mongo.Connect(ctx, opts); err != nil {
		log.Error(mongoLogStr, err)
		return
	}
	log.Infof("mongo connected")

	MongoMgr = &MongoManager{client}
}

func initConfig() {
	config.MongoUri = "mongodb://" + viper.GetString("mongo.addr")
	config.MongoConnectTimeout = viper.GetInt("mongo.connect_timeout")
}

func Close() {
	client.Disconnect(ctx)
}
