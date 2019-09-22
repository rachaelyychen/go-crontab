package test

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/gorhill/cronexpr"
	"os/exec"
	"testing"
	"time"
)

/**
* @project: gocrontab
*
* @description:
*
* @author: cyy
*
* @create: 9/8/19 7:44 PM
**/

func TestCron(t *testing.T) {
	type result struct {
		err    error
		output []byte
	}

	var (
		cmd        *exec.Cmd
		ctx        context.Context
		cancel     context.CancelFunc
		resultChan chan *result
	)

	resultChan = make(chan *result, 1e3)
	ctx, cancel = context.WithCancel(context.TODO())

	go func() {
		var (
			output []byte
			err    error
		)
		cmd = exec.CommandContext(ctx, "-c", "sleep 5;echo $GOPATH")
		output, err = cmd.CombinedOutput()
		resultChan <- &result{
			err:    err,
			output: output,
		}
	}()

	time.Sleep(1 * time.Second)
	cancel()
	res := <-resultChan
	fmt.Println(string(res.output))

	// #########
	var (
		expr     *cronexpr.Expression
		err      error
		nowTime  time.Time
		nextTime time.Time
	)

	if expr, err = cronexpr.Parse("*/5 * * * * * *"); err != nil {
		t.Fatal("failed")
	}

	nowTime = time.Now()
	nextTime = expr.Next(nowTime)
	time.AfterFunc(nextTime.Sub(nowTime), func() {
		fmt.Println("do: ", nextTime)
	})
	time.Sleep(10 * time.Second)

	// #########
	type CronJob struct {
		expr     *cronexpr.Expression
		nextTime time.Time
	}
	var (
		job           *CronJob
		scheduleTable map[string]*CronJob
	)

	nowTime = time.Now()
	scheduleTable = make(map[string]*CronJob)
	expr = cronexpr.MustParse("*/5 * * * * * *")
	job = &CronJob{
		expr:     expr,
		nextTime: expr.Next(nowTime),
	}
	scheduleTable["job1"] = job
	job.expr = cronexpr.MustParse("*/3 * * * * * *")
	job.nextTime = job.expr.Next(time.Now())
	scheduleTable["job2"] = job

	go func() {
		var (
			nowTime time.Time
		)
		for {
			nowTime = time.Now()
			for name, job := range scheduleTable {
				if job.nextTime.Before(nowTime) || job.nextTime.Equal(nowTime) {
					go func(name string) {
						fmt.Println("do: ", name)
					}(name)
					job.nextTime = job.expr.Next(nowTime)
				}
			}

			time.Sleep(100 * time.Millisecond)
		}
	}()

	time.Sleep(100 * time.Second)
}

func TestETCD(t *testing.T) {
	var (
		config             clientv3.Config
		client             *clientv3.Client
		lease              clientv3.Lease
		leaseGrantResp     *clientv3.LeaseGrantResponse
		leaseId            clientv3.LeaseID
		putResp            *clientv3.PutResponse
		getResp            *clientv3.GetResponse
		keepRespChan       <-chan *clientv3.LeaseKeepAliveResponse
		keepResp           *clientv3.LeaseKeepAliveResponse
		kv                 clientv3.KV
		watchStartRevision int64
		watcher            clientv3.Watcher
		watchRespChan      <-chan clientv3.WatchResponse
		err                error
	)

	config = clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}
	if client, err = clientv3.New(config); err != nil {
		t.Fatal(err)
	}
	lease = clientv3.NewLease(client)
	if leaseGrantResp, err = lease.Grant(context.TODO(), 10); err != nil {
		t.Fatal(err)
	}
	leaseId = leaseGrantResp.ID

	if keepRespChan, err = lease.KeepAlive(context.TODO(), leaseId); err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			select {
			case keepResp = <-keepRespChan:
				if keepRespChan == nil {
					goto END
				} else {
					fmt.Println("receive response: ", keepResp)
				}
			}
		}
	END:
	}()

	kv = clientv3.NewKV(client)
	if putResp, err = kv.Put(context.TODO(), "/cron/jobs/job1", "{...}", clientv3.WithLease(leaseId)); err != nil {
		t.Fatal(err)
	}
	putResp = putResp

	for {
		if getResp, err = kv.Get(context.TODO(), "/cron/jobs/job1"); err != nil {
			t.Fatal(err)
		}
		if getResp.Count == 0 {
			fmt.Println("expired...")
			break
		} else {
			fmt.Println("not expierd yet!")
		}
		time.Sleep(2 * time.Second)
	}

	go func() {
		for {
			kv.Put(context.TODO(), "/cron/jobs/job2", "{..}")
			kv.Delete(context.TODO(), "/cron/jobs/job2")
			time.Sleep(2 * time.Second)
		}
	}()
	if getResp, err = kv.Get(context.TODO(), "/cron/jobs/job2"); err != nil {
		t.Fatal(err)
	}
	watchStartRevision = getResp.Header.Revision + 1
	watcher = clientv3.NewWatcher(client)

	ctx, cancelFunc := context.WithCancel(context.TODO())
	time.AfterFunc(5*time.Second, func() {
		cancelFunc()
	})

	watchRespChan = watcher.Watch(ctx, "/cron/jobs/job2", clientv3.WithRev(watchStartRevision))
	for watchResp := range watchRespChan {
		for _, event := range watchResp.Events {
			switch event.Type {
			case mvccpb.PUT:
				fmt.Println("PUT", string(event.Kv.Key), event.Kv.ModRevision)
				break
			case mvccpb.DELETE:
				fmt.Println("DELETE", string(event.Kv.Key), event.Kv.ModRevision)
			}
		}
	}

	var opResp clientv3.OpResponse
	putOp := clientv3.OpPut("/cron/jobs/job3", "{...}")
	if opResp, err = kv.Do(context.TODO(), putOp); err != nil {
		t.Fatal(err)
	}
	fmt.Println(opResp.Put().Header.Revision)

	getOp := clientv3.OpGet("/cron/jobs/job4")
	if opResp, err = kv.Do(context.TODO(), getOp); err != nil {
		t.Fatal(err)
	}
	fmt.Println(opResp.Get().Kvs[0].ModRevision)

	var txnResp *clientv3.TxnResponse
	txn := kv.Txn(context.TODO())
	// if key not exist then create else failed
	txn.If(clientv3.Compare(clientv3.CreateRevision("/cron/lock/job1"), "=", 0)).
		Then(clientv3.OpPut("/cron/lock/job1", "job1", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet("/cron/lock/job1"))
	if txnResp, err = txn.Commit(); err != nil {
		// handler error
	}
	if !txnResp.Succeeded {
		fmt.Println(string(txnResp.Responses[0].GetResponseRange().Kvs[0].Value))
	}
}
