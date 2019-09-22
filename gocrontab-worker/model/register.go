package model

import (
	"context"
	"fmt"
	"github.com/lexkong/log"
	"gocrontab-worker/model/etcd"
	"gocrontab-worker/pkg/errno"
	"gocrontab-worker/utils"
	"net"
)

/**
* @project: gocrontab-worker
*
* @description:
*
* @author: cyy
*
* @create: 9/13/19 1:38 PM
**/

type Register struct {
	localIP string
}

var (
	Reg       *Register
	regLogStr = fmt.Sprintf(utils.LOGSTR, "crontab-worker", "register")
)

func StartRegister() (err error) {
	log.Info("worker's register goroutine starting...")
	var ip string
	if ip, err = getLocalIP(); err != nil {
		ip = "127.0.0.1"
	}
	Reg = &Register{ip}
	Reg.RegistIP()
	return
}

// 获取本机网卡的IP
func getLocalIP() (ipv4 string, err error) {
	var (
		addrs   []net.Addr
		addr    net.Addr
		ipNet   *net.IPNet // IP地址
		isIpNet bool
	)
	// 获取所有网卡
	if addrs, err = net.InterfaceAddrs(); err != nil {
		return
	}
	// 取第一个非lo的网卡IP
	for _, addr = range addrs {
		// 网络地址是IP地址: ipv4, ipv6
		if ipNet, isIpNet = addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() {
			// 跳过IPV6
			if ipNet.IP.To4() != nil {
				// 192.168.1.1
				ipv4 = ipNet.IP.String()
				return
			}
		}
	}
	err = errno.RecordNotFoundErr
	return
}

// worker服务注册
// * worker启动后获取本机网卡IP，作为节点的唯一标识
// * 启动服务注册协程，创建lease并自动续租
// * 带着lease注册到etcd的/cron/worker/{IP}下，供master服务发现
func (reg *Register) RegistIP() {
	var (
		key string
	)
	key = utils.JOB_WORKER_PATH + reg.localIP

	// 不断尝试申请租约 直到成功
	for {
		// 租约时间10秒 不续租的话10秒后会消失
		err := etcd.EtcdMgr.GetLease(context.TODO(), key, 10)
		if err == nil {
			break
		}
	}
}