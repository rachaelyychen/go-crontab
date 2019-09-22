package model

import (
	"fmt"
	"github.com/lexkong/log"
	"gocrontab-worker/utils"
	"time"
)

/**
* @project: gocrontab-worker
*
* @description:
*
* @author: cyy
*
* @create: 9/9/19 9:03 AM
**/

var (
	Sched       *Scheduler
	schedLogStr = fmt.Sprintf(utils.LOGSTR, "crontab-worker", "scheduler")
)

// 调度器
type Scheduler struct {
	JobEventChan  chan *JobEvent  // job event queue 任务事件队列
	JobResultChan chan *JobResult // job result queue 任务结果队列
	JobPlanTable map[string]*JobPlan  // job plan queue 任务计划表
	JobExecTable map[string]*JobState // job state queue 任务执行表
}

func StartScheduler() (err error) {
	Sched = &Scheduler{
		JobEventChan:  make(chan *JobEvent, utils.CAP),
		JobResultChan: make(chan *JobResult, utils.CAP),
		JobPlanTable:  make(map[string]*JobPlan),
		JobExecTable:  make(map[string]*JobState),
	}
	// 启动调度协程
	go Sched.scheduleLoop()
	return
}

func (sched *Scheduler) Close() {
	close(sched.JobEventChan)
	close(sched.JobResultChan)
}

func (sched *Scheduler) PushJobEvent(jobEvent *JobEvent) {
	log.Debugf("%s 任务事件队列接收到任务: %s", schedLogStr, jobEvent.EJob.Name)
	sched.JobEventChan <- jobEvent
}

func (sched *Scheduler) PushJobResult(jobResult *JobResult) {
	log.Debugf("%s 任务结果队列取消任务: %s", schedLogStr, jobResult.State.EJob.Name)
	sched.JobResultChan <- jobResult
}

// 调度协程
// * 监听任务变更event，更新内存中维护的任务列表
// * 检查任务cron表达式，扫描到期任务，交给执行协程去运行
// * 监听任务控制event，强制中断正在执行的shell命令进程
// * 监听任务执行的结果，更新内存中任务状态，将执行日志投递给日志协程
func (sched *Scheduler) scheduleLoop() {
	log.Info("worker's scheduler goroutine starting...")

	var (
		jobEvent  *JobEvent
		jobResult *JobResult
		afterTime time.Duration
		timer     *time.Timer
	)
	// 初始化
	afterTime = sched.reStateJob()
	// 初始化一个定时器
	timer = time.NewTimer(afterTime)

	for {
		select {
		case jobEvent = <-sched.JobEventChan:	
			// 维护任务事件队列
			sched.handlerJobEvent(jobEvent)
		case <-timer.C:	// 定时器提醒有任务到期了
		case jobResult = <-sched.JobResultChan:
			sched.handlerJobResult(jobResult)
			continue
		}
		// 巡检计划表
		afterTime = sched.reStateJob()
		// 重设定时器
		timer.Reset(afterTime)
	}
	return
}

func (sched *Scheduler) handlerJobEvent(jobEvent *JobEvent) {
	var (
		jobPlan  *JobPlan
		jobState *JobState
		jobName  string
		err      error
		exists   bool
	)
	jobName = jobEvent.EJob.Name

	switch jobEvent.Type {
	case utils.JOB_SAVE_EVENT: // 更新任务
		if jobPlan, err = GetJobPlan(jobEvent.EJob); err != nil {
			log.Error(schedLogStr, err)
			return
		}
		sched.JobPlanTable[jobName] = jobPlan
		log.Infof("%s 处理更新任务,已添加任务 %s 至任务计划表", schedLogStr, jobName)
	case utils.JOB_DEL_EVENT: // 删除任务
		// 删除一个不存在的任务,etcd还是会产生delete事件,因此这里需要判断一下推送过来删除的任务是否存在
		if jobPlan, exists = sched.JobPlanTable[jobName]; exists {
			// 存在则从任务计划表中删除
			delete(sched.JobPlanTable, jobName)
			log.Infof("%s 处理删除任务,已从任务计划表中删除任务 %s", schedLogStr, jobName)
		}
	case utils.JOB_KILL_EVENT: // 终止任务
		// 需要判断一下要终止的任务是否正在执行
		if jobState, exists = sched.JobExecTable[jobName]; exists {
			// 正在执行才终止它 直接调用取消函数即可 Execute()函数中已配置cancel context
			jobState.CancelFunc()
			log.Infof("%s 处理终止任务,已终止任务 %s 执行", schedLogStr, jobName)
		}
	}
}

func (sched *Scheduler) handlerJobResult(jobResult *JobResult) {
	// 任务执行结束后,从任务执行表中删除该任务
	delete(sched.JobExecTable, jobResult.State.EJob.Name)
	// 生成任务日志
	if jobResult.Done {
		jobLog := &JobLog{
			JobName:      jobResult.State.EJob.Name,
			Command:      jobResult.State.EJob.Command,
			Output:       string(jobResult.Output),
			PlanTime:     jobResult.State.PlanTime.UnixNano() / 1e6, // 日志时间精确到毫秒
			ScheduleTime: jobResult.State.RealTime.UnixNano() / 1e6,
			StartTime:    jobResult.StartTime.UnixNano() / 1e6,
			EndTime:      jobResult.EndTime.UnixNano() / 1e6,
			UseTime:      jobResult.EndTime.Sub(jobResult.StartTime).Seconds(),
		}
		if jobResult.Err != nil {
			jobLog.Err = jobResult.Err.Error()
		} else {
			jobLog.Err = ""
		}
		// save logs
		PushJobLog(jobLog)
	}
}

func (sched *Scheduler) reStateJob() (afterTime time.Duration) {
	if len(sched.JobPlanTable) == 0 {
		// 没有任务就设置为1秒
		afterTime = 1 * time.Second
		return
	}
	// 1.扫描计划表中的所有任务,过期的任务立即执行
	// 2.找到最近的要过期任务的时间
	var t *time.Time
	nowTime := time.Now()
	for _, jobPlan := range sched.JobPlanTable {
		name := jobPlan.EJob.Name
		if jobPlan.NextTime.Before(nowTime) || jobPlan.NextTime.Equal(nowTime) {
			log.Infof("%s 正在尝试执行任务: %s", schedLogStr, name)
			// try to do this job
			sched.doJob(jobPlan)
			// 更新job下次执行时间
			jobPlan.NextTime = jobPlan.Expr.Next(nowTime)
			log.Infof("%s 任务%s的下一次执行时间: %+v", schedLogStr, name, jobPlan.NextTime)
		}
		if t == nil || jobPlan.NextTime.Before(*t) {
			t = &jobPlan.NextTime
		}
	}
	// 下次调度间隔（最近要执行的任务调度时间-当前时间）
	afterTime = (*t).Sub(nowTime)
	log.Debugf("%s %+v后开始执行下一个任务", schedLogStr, afterTime)
	return
}

func (sched *Scheduler) doJob(jobPlan *JobPlan) () {
	// 例如设置一个任务每秒调度一次,该任务每次执行需要1分钟
	// 在执行过程中不会因为调度而重新执行,因此需要记录任务的状态,防止并发
	var (
		jobState *JobState
		exists   bool
		name     string
	)
	name = jobPlan.EJob.Name
	// 如果正在执行,就跳过
	if jobState, exists = sched.JobExecTable[name]; exists {
		log.Debugf("任务 %s 还在执行中,请等待", name)
		return
	}
	// 获取任务状态,放入任务状态表中
	jobState = GetJobState(jobPlan)
	sched.JobExecTable[name] = jobState
	// do this job
	Exec.ExecuteJob(jobState)
}
