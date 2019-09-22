package main

import (
	"github.com/spf13/pflag"
	"gocrontab-worker/config"
	"gocrontab-worker/model"
	"gocrontab-worker/model/etcd"
	"gocrontab-worker/model/mongo"
	"gocrontab-worker/service"
	"runtime"
	"time"
)

/**
* @project: gocrontab-worker
*
* @description:
*
* @author: cyy
*
* @create: 8/31/19 12:11 PM
**/

var (
	cfg = pflag.StringP("conf", "c", "", "config file path.")
)

func main() {
	// 初始化线程数量
	runtime.GOMAXPROCS(runtime.NumCPU())
	pflag.Parse()

	// init config
	if err := config.Init(*cfg); err != nil {
		panic(err)
	}

	// init redis
	//redis.Init()
	//defer redis.Close()

	// init mongo
	mongo.Init()
	defer mongo.Close()

	// init etcd
	etcd.Init()
	defer etcd.Close()

	// start service register
	model.StartRegister()

	// start log handler
	model.StartLogger()

	// start job executor
	model.StartExecutor()

	// start job scheduler
	model.StartScheduler()
	defer model.Sched.Close()

	// start job watcher
	service.WatchJobs()

	// 正常退出
	for {
		time.Sleep(1 * time.Second)
	}
}
