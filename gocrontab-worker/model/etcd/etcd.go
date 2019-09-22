package etcd

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/lexkong/log"
	"github.com/spf13/viper"
	"gocrontab-worker/pkg/errno"
	"gocrontab-worker/utils"
	"strings"
	"time"
)

/**
* @project: gocrontab-worker
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
	etcdLogStr = fmt.Sprintf(utils.LOGSTR, "crontab-worker", "etcd")
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

func (etcdMgr *EtcdManager) GetLease(ctx context.Context, key string, leaseTime int) (err error) {
	var (
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId        clientv3.LeaseID
		keepResp       *clientv3.LeaseKeepAliveResponse
		keepRespChan   <-chan *clientv3.LeaseKeepAliveResponse
	)

	// 创建一个租约
	if leaseGrantResp, err = etcdMgr.Lease.Grant(ctx, int64(leaseTime)); err != nil {
		log.Error(etcdLogStr, err)
		return
	}

	// 创建一个context用于取消续租
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	// 获取租约ID
	leaseId = leaseGrantResp.ID

	// 自动续租
	if keepRespChan, err = etcdMgr.Lease.KeepAlive(cancelCtx, leaseId); err != nil {
		log.Error(etcdLogStr, err)
		goto FAIL
	}

	// 用一个goroutine处理续租应答
	go func() {
		for {
			select {
			case keepResp = <-keepRespChan:
				// 续租失败就退出
				if keepResp == nil {
					goto END
				}
			}
		}
	END:
	}()

	if _, err = etcdMgr.Kv.Put(cancelCtx, key, "", clientv3.WithLease(leaseId)); err != nil {
		log.Error(etcdLogStr, err)
		goto FAIL
	}
	return

FAIL:
	cancelFunc()                                  // 取消自动续租
	etcdMgr.Lease.Revoke(context.TODO(), leaseId) //  释放租约
	return
}

func (etcdMgr *EtcdManager) GetOptLock(ctx context.Context, key string, leaseTime int) (optLock *OptLock, err error) {
	var (
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId        clientv3.LeaseID
		keepResp       *clientv3.LeaseKeepAliveResponse
		keepRespChan   <-chan *clientv3.LeaseKeepAliveResponse
		txn            clientv3.Txn
		txnResp        *clientv3.TxnResponse
	)

	// 创建一个租约
	if leaseGrantResp, err = etcdMgr.Lease.Grant(ctx, int64(leaseTime)); err != nil {
		log.Error(etcdLogStr, err)
		return
	}

	// 创建一个context用于取消续租
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	// 获取租约ID
	leaseId = leaseGrantResp.ID

	// 自动续租
	if keepRespChan, err = etcdMgr.Lease.KeepAlive(cancelCtx, leaseId); err != nil {
		log.Error(etcdLogStr, err)
		goto FAIL
	}

	// 用一个goroutine处理续租应答
	go func() {
		for {
			select {
			case keepResp = <-keepRespChan:
				// 续租失败就退出
				if keepResp == nil {
					goto END
				}
			}
		}
	END:
	}()

	// 拿着租约抢占key,如果能抢到就可以处理业务
	txn = etcdMgr.Kv.Txn(cancelCtx)
	// if key not exist then create else failed
	txn.If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0, )).
		Then(clientv3.OpPut(key, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(key))
	if txnResp, err = txn.Commit(); err != nil {
		log.Error(etcdLogStr, err)
		goto FAIL
	}
	// 没抢到,记录下谁抢到了锁
	if !txnResp.Succeeded {
		log.Debugf("%s 抢到了锁%s", etcdLogStr, string(txnResp.Responses[0].GetResponseRange().Kvs[0].Value))
		err = errno.LockBusyErr
		goto FAIL
	}

	// 否则抢到了 返回这个锁 用于处理业务
	optLock = &OptLock{
		Key:        key,
		CancelFunc: cancelFunc,
		LeaseId:    leaseId,
	}
	return

FAIL:
	cancelFunc()                                  // 取消自动续租
	etcdMgr.Lease.Revoke(context.TODO(), leaseId) //  释放租约
	return
}

func (etcdMgr *EtcdManager) ReleaseOptLock(optLock *OptLock) {
	// 自动取消续约 释放租约(关联的键值对同时被删除)也就立即释放锁
	optLock.CancelFunc()
	etcdMgr.Lease.Revoke(context.TODO(), optLock.LeaseId)
}
