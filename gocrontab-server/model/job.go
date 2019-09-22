package model

import (
	"context"
	"fmt"
	"github.com/gorhill/cronexpr"
	"github.com/lexkong/log"
	"gocrontab-server/utils"
	"time"
)

/**
* @project: gocrontab-server
*
* @description:
*
* @author: cyy
*
* @create: 9/8/19 11:47 AM
**/

var (
	jobLogStr = fmt.Sprintf(utils.LOGSTR, "crontab-master", "job")
)

type Job struct {
	Name     string `json:"name"`      // 任务名,不重复
	Command  string `json:"command"`   // shell命令
	CronExpr string `json:"cron_expr"` // cron表达式
}

// 任务变化的事件有两种：修改任务、删除任务
type JobEvent struct {
	Type int  // 0:save, 1:delete
	EJob *Job // 任务
}

// 任务调度计划
type JobPlan struct {
	EJob     *Job                 // 任务
	Expr     *cronexpr.Expression // 已解析的cron表达式
	NextTime time.Time            // 任务的下一次执行时间
}

// 任务的状态,用于判断任务是否正在执行
type JobState struct {
	EJob       *Job               // 任务
	PlanTime   time.Time          // 理论上的调度时间
	RealTime   time.Time          // 实际的调度时间
	CancelCtx  context.Context    // 用于取消任务的context
	CancelFunc context.CancelFunc // 用于取消任务的取消函数
}

// 任务执行结果
type JobResult struct {
	State     *JobState // 任务的状态
	Output    []byte    // 执行的输出结果
	Err       error     // 执行命令发生的错误
	StartTime time.Time // 执行开始时间
	EndTime   time.Time // 执行结束时间
	Done      bool      // 是否执行
}

// 任务日志
type JobLog struct {
	JobName      string  `json:"job_name" bson:"job_name"`
	Command      string  `json:"command" bson:"command"`
	Err          string  `json:"err" bson:"err"` // 任务执行的错误
	Output       string  `json:"output" bson:"output"`
	PlanTime     int64   `json:"plan_time" bson:"plan_time"`         // 计划时间
	ScheduleTime int64   `json:"schedule_time" bson:"schedule_time"` // 调度时间
	StartTime    int64   `json:"start_time" bson:"start_time"`       // 执行开始时间
	EndTime      int64   `json:"end_time" bson:"end_time"`           // 执行结束时间
	UseTime      float64 `json:"use_time" bson:"use_time"`           // 执行用时
}

func GetJobEvent(job *Job, t int) *JobEvent {
	return &JobEvent{t, job}
}

func GetJobPlan(job *Job) (jobPlan *JobPlan, err error) {
	var (
		expr *cronexpr.Expression
	)
	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		log.Error(jobLogStr, err)
		return
	}
	jobPlan = &JobPlan{
		EJob:     job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}

func GetJobState(jobPlan *JobPlan) (jobState *JobState) {
	jobState = &JobState{
		EJob:     jobPlan.EJob,
		PlanTime: jobPlan.NextTime,
		RealTime: time.Now(),
	}
	jobState.CancelCtx, jobState.CancelFunc = context.WithCancel(context.TODO())
	return
}
