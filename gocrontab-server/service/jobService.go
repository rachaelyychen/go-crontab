// master提供管理任务（CRUD）接口，查看任务日志接口，停止任务接口
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/lexkong/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gocrontab-server/model"
	"gocrontab-server/model/etcd"
	"gocrontab-server/utils"
)

/**
* @project: gocrontab-server
*
* @description:
*
* @author: cyy
*
* @create: 9/8/19 11:51 AM
**/

var (
	jobLogStr = fmt.Sprintf(utils.LOGSTR, "crontab-master", "jobService")
)

func AddJob(job *model.Job) (oldJob *model.Job, err error) {
	// 任务保存到etcd,key=/cron/job/{job_name},value=JSON string
	var (
		jobKey   string
		jobValue []byte
		putResp   *clientv3.PutResponse
	)
	jobKey = utils.JOB_SAVE_PATH + job.Name
	if jobValue, err = json.Marshal(job); err != nil {
		log.Errorf(err, "failed to marshal job: %+v", job)
		return
	}
	// save to etcd
	if putResp, err = etcd.EtcdMgr.Kv.Put(context.TODO(), jobKey, string(jobValue),
		clientv3.WithPrevKV()); err != nil {
		log.Error(jobLogStr, err)
		return
	}
	oldJob = &model.Job{}
	if putResp.PrevKv != nil {
		if err = json.Unmarshal(putResp.PrevKv.Value, oldJob); err != nil {
			log.Error(jobLogStr, err)
			err = nil
			return
		}
	}
	return
}

func DeleteJob(name string) (oldJob *model.Job, err error) {
	var (
		jobKey     string
		deleteResp *clientv3.DeleteResponse
	)
	jobKey = utils.JOB_SAVE_PATH + name
	// delete from etcd
	if deleteResp, err = etcd.EtcdMgr.Kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV());
		err != nil {
		log.Error(jobLogStr, err)
		return
	}
	oldJob = &model.Job{}
	if len(deleteResp.PrevKvs) > 0 {
		if err = json.Unmarshal(deleteResp.PrevKvs[0].Value, oldJob); err != nil {
			log.Error(jobLogStr, err)
			err = nil
			return
		}
	}
	return
}

func GetJobs() (jobList []*model.Job, err error) {
	var (
		jobKey  string
		getResp *clientv3.GetResponse
	)
	jobKey = utils.JOB_SAVE_PATH
	if getResp, err = etcd.EtcdMgr.Kv.Get(context.TODO(), jobKey, clientv3.WithPrefix()); err != nil {
		log.Error(jobLogStr, err)
		return
	}
	jobList = make([]*model.Job, 0, utils.CAP)
	for _, kv := range getResp.Kvs {
		var job model.Job
		if err = json.Unmarshal(kv.Value, &job); err != nil {
			log.Error(jobLogStr, err)
			err = nil
			continue
		}
		jobList = append(jobList, &job)
	}
	return
}

func KillJob(name string) (err error) {
	var (
		killJobKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
	)
	killJobKey = utils.JOB_KILL_PATH + name
	// 只需要让worker监听到一次put即可,不需要让killJobKey一直保留在etcd中
	// 因此绑定一个租约让其稍后自动过期,可以自动删除killJobKey
	if leaseGrantResp, err = etcd.EtcdMgr.Lease.Grant(context.TODO(), 2); err != nil {
		log.Error(jobLogStr, err)
		return
	}
	if _, err = etcd.EtcdMgr.Kv.Put(context.TODO(), killJobKey, "", clientv3.WithLease(leaseGrantResp.ID));
		err != nil {
		log.Error(jobLogStr, err)
		return
	}
	return
}

func GetJobLogs(name string, page int64, pageSize int64, sortStr string) ([]*model.JobLog, error) {
	var (
		filter *bson.M
		opts *options.FindOptions
		skip int64
		sort map[string]interface{}
	)
	filter = &bson.M{"job_name":name}
	skip = (page-1) * pageSize
	sort = make(map[string]interface{})
	sort[sortStr] = -1

	opts = &options.FindOptions{
		Skip: &skip,
		Sort: &sort,
		Limit: &pageSize,
	}
	return model.GetLogBatch(filter, opts), nil
}
