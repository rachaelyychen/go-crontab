package main

import (
	"github.com/gin-gonic/gin"
	"github.com/lexkong/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"gocrontab-server/config"
	"gocrontab-server/model/etcd"
	mongo2 "gocrontab-server/model/mongo"
	"gocrontab-server/routers"
	"net/http"
	"runtime"
)

/**
* @project: gocrontab-server
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
	mongo2.Init()
	defer mongo2.Close()

	// init etcd
	etcd.Init()
	defer etcd.Close()

	// start gin
	log.Infof("Start to init Routers.")
	gin.SetMode(viper.GetString("runmode"))
	g := gin.New()
	routers.Load(g)

	http.ListenAndServe(viper.GetString("port"), g)
}
