package model

import (
	"context"
	"fmt"
	"github.com/lexkong/log"
	"gocrontab-worker/model/etcd"
	"gocrontab-worker/utils"
	"math/rand"
	"os/exec"
	"time"
)

/**
* @project: gocrontab-worker
*
* @description:
*
* @author: cyy
*
* @create: 9/9/19 8:32 PM
**/

type Executor struct {

}

type JobLock struct {
	JobName string
}

var (
	Exec       *Executor
	execLogStr = fmt.Sprintf(utils.LOGSTR, "crontab-worker", "executor")
)

func StartExecutor() (err error) {
	log.Info("worker's executor goroutine starting...")
	Exec = &Executor{}
	return
}

// 执行协程
// * 在etcd中抢占分布式乐观锁：/cron/lock/job_name
// * 抢锁成功则通过Command类执行shell任务
// * 捕获Command输出并等待子进程结束，将执行结果投递给调度协程
func (exec *Executor) ExecuteJob(jobState *JobState) {
	go func() {
		var (
			jobLock   *etcd.OptLock
			jobResult *JobResult
			err       error
		)

		// 上锁之前随机睡眠0～1秒 减少多个机器时钟不完全同步造成的分布式锁倾斜
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		// 抢占锁,锁租约时间5秒
		jobLock, err = etcd.EtcdMgr.GetOptLock(context.TODO(), utils.JOB_LOCK_PATH + jobState.EJob.Name, 5)
		// 结束释放锁
		if jobLock != nil {
			defer etcd.EtcdMgr.ReleaseOptLock(jobLock)
		}

		nowTime := time.Now()
		// 初始化一个任务结果
		jobResult = &JobResult{
			State:     jobState,
			Output:    make([]byte, 0),
			Err:       nil,
			StartTime: nowTime,
			EndTime:   nowTime,
			Done:      false,
		}

		if err != nil {
			log.Debugf("%s 本次任务未执行,原因是%+v", execLogStr, err)
		} else {
			// 执行任务
			execute(jobState, jobResult)
		}
		// 任务执行结束后，返回执行结果给调度器，调度器会从任务执行表中删除该任务
		Sched.PushJobResult(jobResult)
	}()
}

func execute(jobState *JobState, jobResult *JobResult) {
	log.Infof("%s 正在执行任务: %s", execLogStr, jobState.EJob.Name)

	var (
		cmd    *exec.Cmd
		err    error
		output []byte
	)

	// 调用/bin/bash执行命令,捕获输出
	cmd = exec.CommandContext(jobState.CancelCtx, "/bin/bash", "-c", jobState.EJob.Command)
	output, err = cmd.CombinedOutput()
	jobResult.EndTime = time.Now()
	jobResult.Output = output
	jobResult.Err = err
	jobResult.Done = true
	log.Infof("%s 任务 %s 执行结束,用时: %+v, 执行结果: %+v,出现错误: %+v", execLogStr,
		jobState.EJob.Name, jobResult.EndTime.Sub(jobResult.StartTime),
		string(jobResult.Output), jobResult.Err)
	return
}
