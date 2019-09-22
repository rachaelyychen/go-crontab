package etcd

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/lexkong/log"
	"github.com/spf13/viper"
	"gocrontab-server/pkg/errno"
	"gocrontab-server/utils"
	"strings"
	"time"
)

/**
* @project: gocrontab-server
*
* @description:
*
* @author: cyy
*
* @create: 9/8/19 11:53 AM
**/

const (
	DefaultEtcdAddr        = "localhost:2379"
	DefaultEtcdDialTimeout = 5000
)

var (
	etcdLogStr = fmt.Sprintf(utils.LOGSTR, "crontab-master", "etcd")
	client     *clientv3.Client
	config     clientv3.Config
	err        error
	EtcdMgr    *EtcdManager
)

type OptLock struct {
	Key        string
	CancelFunc context.CancelFunc
	LeaseId    clientv3.LeaseID
}

type EtcdManager struct {
	Client  *clientv3.Client
	Kv      clientv3.KV
	Lease   clientv3.Lease
	Watcher clientv3.Watcher
}

func Init() {
	endpoints := strings.Split(viper.GetString("etcd.endpoints"), ",")
	dialTimeout := viper.GetInt64("etcd.dial_timeout")
	config = clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Duration(dialTimeout) * time.Second,
	}

	if client, err = clientv3.New(config); err != nil {
		log.Errorf(err, "failed to build etcd connection, endpoints: %+v", endpoints)
		return
	}

	log.Info("etcd connected")

	EtcdMgr = &EtcdManager{
		Client:  client,
		Kv:      clientv3.NewKV(client),
		Lease:   clientv3.NewLease(client),
		Watcher: clientv3.NewWatcher(client),
	}
}

func Close() {
	client.Close()
}

func (etcdMgr *EtcdManager) GetOptLock(name string) (optLock *OptLock, err error) {
	var (
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId        clientv3.LeaseID
		keepRespChan   <-chan *clientv3.LeaseKeepAliveResponse
		key            string
		txn            clientv3.Txn
		txnResp        *clientv3.TxnResponse
	)

	// 创建一个5秒的租约
	if leaseGrantResp, err = etcdMgr.Lease.Grant(context.TODO(), utils.LEASE_TIME); err != nil {
		log.Error(etcdLogStr, err)
		return
	}

	key = utils.JOB_LOCK_PATH + name
	// 创建一个context用于取消续租
	ctx, cancelFunc := context.WithCancel(context.TODO())
	// 获取租约ID
	leaseId = leaseGrantResp.ID

	optLock = &OptLock{
		Key:        key,
		CancelFunc: cancelFunc,
		LeaseId:    leaseId,
	}

	// 自动续租
	if keepRespChan, err = etcdMgr.Lease.KeepAlive(ctx, leaseId); err != nil {
		log.Error(etcdLogStr, err)
		return
	}

	// 用一个goroutine处理续租应答
	go func() {
		for {
			select {
			case <-keepRespChan:
				// 续租失败就退出
				if keepRespChan == nil {
					goto END
				}
			}
		}
	END:
	}()

	// 拿着租约抢占key,如果能抢到就可以处理业务
	txn = etcdMgr.Kv.Txn(context.TODO())
	// if key not exist then create else failed
	txn.If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(key))
	if txnResp, err = txn.Commit(); err != nil {
		log.Error(etcdLogStr, err)
		return
	}
	// 没抢到,记录下谁抢到了锁
	if !txnResp.Succeeded {
		log.Debugf("%s 抢到了锁%s", etcdLogStr, string(txnResp.Responses[0].GetResponseRange().Kvs[0].Value))
		err = errno.LockBusyErr
		return
	}

	// 否则抢到了 返回这个锁 用于处理业务
	return
}

func (etcdMgr *EtcdManager) ReleaseOptLock(optLock *OptLock) {
	// 自动取消续约 释放租约(关联的键值对同时被删除)也就立即释放锁
	optLock.CancelFunc()
	etcdMgr.Lease.Revoke(context.TODO(), optLock.LeaseId)
}
