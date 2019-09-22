// worker提供同步任务、调度任务、执行任务、保存日志的功能
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/lexkong/log"
	"gocrontab-worker/model"
	"gocrontab-worker/model/etcd"
	"gocrontab-worker/utils"
	"strings"
)

/**
* @project: gocrontab-worker
*
* @description:
*
* @author: cyy
*
* @create: 9/8/19 9:32 PM
**/

var (
	jobServiceStr = fmt.Sprintf(utils.LOGSTR, "crontab-worker", "jobService")
)

// 监听协程
// * 利用watch API，监听/cron/jobs/和/cron/killer/目录的变化
// * 将变化事件通过channel推送给调度协程
func WatchJobs() (err error) {
	log.Info("worker's watcher goroutine starting...")

	var (
		getResp            *clientv3.GetResponse
		watchStartRevision int64
		watchChan          clientv3.WatchChan
		jobEvent           *model.JobEvent
		jobName            string
	)

	// 1.get到/cron/job/目录下的所有任务
	if getResp, err = etcd.EtcdMgr.Kv.Get(context.TODO(), utils.JOB_SAVE_PATH, clientv3.WithPrefix());
		err != nil {
		log.Error(jobServiceStr, err)
		return
	}
	for _, kv := range getResp.Kvs {
		job := &model.Job{}
		// 把job投递给调度协程
		if err = json.Unmarshal(kv.Value, job); err == nil {
			jobEvent = model.GetJobEvent(job, utils.JOB_SAVE_EVENT)
			model.Sched.PushJobEvent(jobEvent)
		}
	}

	// 监听/cron/jobs/
	go func() {
		// 2.获取当前集群的revision
		// 3.从该revision的下一个版本开始监听变化
		watchStartRevision = getResp.Header.Revision + 1
		watchChan = etcd.EtcdMgr.Watcher.Watch(context.TODO(), utils.JOB_SAVE_PATH,
			clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())

		log.Infof("%s 监控从revision = %d开始监听变化", jobServiceStr, watchStartRevision)

		for watchResp := range watchChan {
			for _, event := range watchResp.Events {
				job := &model.Job{}
				switch event.Type {
				case mvccpb.PUT:
					// 把修改job封装成一个event,投递给调度协程
					if err = json.Unmarshal(event.Kv.Value, job); err != nil {
						log.Error(jobServiceStr, err)
						continue
					}
					jobEvent = &model.JobEvent{utils.JOB_SAVE_EVENT, job}
				case mvccpb.DELETE:
					// 把删除job封装成一个event,推送给调度协程
					jobName = strings.TrimPrefix(string(event.Kv.Key), utils.JOB_SAVE_PATH)
					job = &model.Job{Name: jobName}
					jobEvent = &model.JobEvent{utils.JOB_DEL_EVENT, job}
				}
				// send to scheduler
				model.Sched.PushJobEvent(jobEvent)
			}
		}
	}()

	// 监听/cron/killer/
	go func() {
		watchChan = etcd.EtcdMgr.Watcher.Watch(context.TODO(), utils.JOB_KILL_PATH, clientv3.WithPrefix())
		for watchResp := range watchChan {
			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					// 杀死job10 key=/cron/killer/job10
					jobName = strings.TrimPrefix(string(event.Kv.Key), utils.JOB_KILL_PATH)
					job := &model.Job{Name: jobName}
					jobEvent = &model.JobEvent{utils.JOB_KILL_EVENT, job}
				case mvccpb.DELETE:
				}
				// send to scheduler
				model.Sched.PushJobEvent(jobEvent)
			}
		}
	}()

	return
}
