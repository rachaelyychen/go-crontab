package model

import (
	"context"
	"fmt"
	"github.com/lexkong/log"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	mongo2 "gocrontab-worker/model/mongo"
	"gocrontab-worker/utils"
	"sync"
	"time"
)

/**
* @project: gocrontab-worker
*
* @description:
*
* @author: cyy
*
* @create: 9/12/19 10:52 AM
**/

var (
	jobServiceStr    = fmt.Sprintf(utils.LOGSTR, "crontab-worker", "logger")
	jobLogCollection *mongo.Collection
	jobLogChan       chan *JobLog
	jobLogBatch      []interface{}
	pushTimer        *time.Timer // 延迟提交的定时器
	log_lazy_time    = viper.GetInt("log_lazy_time")
)

// 日志协程
// * 监听调度协程发来的执行日志，放入一个batch中
// * 对新batch启动定时器，设置超时未满自动提交(lazy commit time)
// * batch如果放满，就立刻提交，并取消自动提交的定时器
func StartLogger() {
	go func() {
		log.Info("worker's logger goroutine starting...")

		jobLogCollection = mongo2.MongoMgr.Client.Database(utils.MON_DATABASE_CRON).Collection(utils.MON_COLLECTION_LOG)
		jobLogChan = make(chan *JobLog, utils.CAP)

		for {
			select {
			case jobLog := <-jobLogChan:
				log.Debugf("%s logger receive a log: %+v", jobServiceStr, *jobLog)
				if jobLogBatch == nil {
					jobLogBatch = make([]interface{}, 0, utils.CAP)
					pushTimer = time.AfterFunc(5*time.Second, func() {
						log.Debugf("%s 触发了定时器来提交任务日志\n", jobServiceStr)
						handleLogs()
					})
				}
				if len(jobLogBatch) >= utils.BATCH {
					handleLogs()
				}
				// push log into batch
				jobLogBatch = append(jobLogBatch, jobLog)
			}
		}
	}()
	return
}

func PushJobLog(jobLog *JobLog) {
	// TODO MQ
	jobLogChan <- jobLog
}

func handleLogs() {
	var (
		mutex sync.Mutex
	)
	mutex.Lock()
	{
		// push batch into mongo 忽略可能的push失败
		PushLogBatch(&jobLogBatch)
		// clear log batch
		jobLogBatch = make([]interface{}, 0, utils.CAP)
		// reset timer
		pushTimer.Reset(5 * time.Second)
	}
	mutex.Unlock()
}

func PushLogBatch(batch *[]interface{}) {
	if _, err := jobLogCollection.InsertMany(context.TODO(), *batch); err != nil {
		log.Infof("%s job log batch insert %+v", jobServiceStr, err)
	}
	return
}

func GetLogBatch(filter *bson.M, opts *options.FindOptions) (batch []*JobLog) {
	var (
		cur    *mongo.Cursor
		jobLog *JobLog
		err    error
	)
	batch = make([]*JobLog, 0, utils.CAP)

	if cur, err = jobLogCollection.Find(context.TODO(), *filter, opts); err != nil {
		log.Error(jobServiceStr, err)
	}
	defer cur.Close(context.TODO())

	for cur.Next(context.TODO()) {
		jobLog = &JobLog{}
		cur.Decode(jobLog)
		batch = append(batch, jobLog)
	}
	return
}

/*
// 日志协程的另一种解决方法
	for {
		select {
		case log = <- logSink.logChan:
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				// 让这个批次超时自动提交(给1秒的时间）
				commitTimer = time.AfterFunc(
					time.Duration(G_config.JobLogCommitTimeout) * time.Millisecond,
					func(batch *common.LogBatch) func() {
						return func() {
							logSink.autoCommitChan <- batch
						}
					}(logBatch),
				)
			}

			// 把新日志追加到批次中
			logBatch.Logs = append(logBatch.Logs, log)

			// 如果批次满了, 就立即发送
			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				// 发送日志
				logSink.saveLogs(logBatch)
				// 清空logBatch
				logBatch = nil
				// 取消定时器
				commitTimer.Stop()
			}
		case timeoutBatch = <- logSink.autoCommitChan: // 过期的批次
			// 判断过期批次是否仍旧是当前的批次
			if timeoutBatch != logBatch {
				continue // 跳过已经被提交的批次
			}
			// 把批次写入到mongo中
			logSink.saveLogs(timeoutBatch)
			// 清空logBatch
			logBatch = nil
		}
	}
*/
