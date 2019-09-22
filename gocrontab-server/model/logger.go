package model

import (
	"context"
	"fmt"
	"github.com/lexkong/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	mongo2 "gocrontab-server/model/mongo"
	"gocrontab-server/utils"
)

/**
* @project: gocrontab-server
*
* @description:
*
* @author: cyy
*
* @create: 9/12/19 10:52 AM
**/

var (
	loggerLogStr = fmt.Sprintf(utils.LOGSTR, "crontab-master", "logger")
)

func GetLogBatch(filter *bson.M, opts *options.FindOptions) (batch []*JobLog) {
	var (
		cur              *mongo.Cursor
		jobLog           *JobLog
		err              error
		jobLogCollection = mongo2.MongoMgr.Client.Database(utils.MON_DATABASE_CRON).Collection(utils.MON_COLLECTION_LOG)
	)
	batch = make([]*JobLog, 0, utils.CAP)

	if cur, err = jobLogCollection.Find(context.TODO(), *filter, opts); err != nil {
		log.Error(loggerLogStr, err)
	}
	defer cur.Close(context.TODO())

	for cur.Next(context.TODO()) {
		jobLog = &JobLog{}
		cur.Decode(jobLog)
		batch = append(batch, jobLog)
	}
	return
}
