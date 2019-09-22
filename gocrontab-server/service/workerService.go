package service

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/lexkong/log"
	"gocrontab-server/model/etcd"
	"gocrontab-server/utils"
	"strings"
)

/**
* @project: gocrontab-server
*
* @description:
*
* @author: cyy
*
* @create: 9/13/19 3:07 PM
**/

var (
	workerLogStr = fmt.Sprintf(utils.LOGSTR, "crontab-master", "workerService")
)

func GetHWorkers() (workerList []string, err error) {
	var (
		workerKey string
		getResp   *clientv3.GetResponse
	)
	workerKey = utils.JOB_WORKER_PATH
	if getResp, err = etcd.EtcdMgr.Kv.Get(context.TODO(), workerKey, clientv3.WithPrefix()); err != nil {
		log.Error(workerLogStr, err)
		return
	}
	workerList = make([]string, 0, utils.CAP)
	for _, kv := range getResp.Kvs {
		// kv.Key = /cron/worker/192.168.106.100
		workerList = append(workerList, strings.TrimPrefix(string(kv.Key), utils.JOB_WORKER_PATH))
	}

	return
}
