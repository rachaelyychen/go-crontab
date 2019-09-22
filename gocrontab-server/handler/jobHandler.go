package handler

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/lexkong/log"
	"gocrontab-server/model"
	"gocrontab-server/pkg/errno"
	"gocrontab-server/service"
	"gocrontab-server/utils"
)

var jobLogStr = fmt.Sprintf(utils.LOGSTR, "crontab-master", "jobHandler")

// @Summary 获取任务
// @Accept
// @Produce  json
// @Success 200 {} json "{}"
// @Router /crontab/job/query [post]
func GetJobs(c *gin.Context) {
	jobList, err := service.GetJobs()
	if err != nil {
		SendResponse(c, errno.ErrGet, nil)
		return
	}
	SendResponse(c, errno.OK, jobList)
}

// @Summary 添加任务
// @Accept json
// @Produce  json
// @Success 200 {} json "{}"
// @Router /crontab/job [post]
func CreateJob(c *gin.Context) {
	var job model.Job
	if err := c.Bind(&job); err != nil {
		log.Error(jobLogStr, err)
		SendResponse(c, errno.ErrBind, nil)
		return
	}
	log.Infof("%s add job %+v", jobLogStr, job)
	oldJob, err := service.AddJob(&job)
	if err != nil {
		SendResponse(c, errno.ErrInsert, nil)
		return
	}
	SendResponse(c, errno.OK, *oldJob)
}

// @Summary 删除任务
// @Accept form-data
// @Produce  json
// @Success 200 {} json "{}"
// @Router /crontab/job [delete]
func DeleteJob(c *gin.Context) {
	name := c.PostForm("name")
	log.Infof("%s delete job name: %s", jobLogStr, name)
	oldJob, err := service.DeleteJob(name)
	if err != nil {
		SendResponse(c, errno.ErrUpdate, nil)
		return
	}
	SendResponse(c, errno.OK, *oldJob)
}

// @Summary 取消任务
// @Accept form-data
// @Produce  json
// @Success 200 {} json "{}"
// @Router /crontab/job/killer [post]
func KillJob(c *gin.Context) {
	name := c.PostForm("name")
	log.Infof("%s try to kill job: %s", jobLogStr, name)
	if err := service.KillJob(name); err != nil {
		SendResponse(c, errno.ErrKill, nil)
		return
	}
	SendResponse(c, errno.OK, nil)
}

// @Summary 获取任务日志
// @Accept	json
// @Produce  json
// @Success 200 {} json "{}"
// @Router /crontab/job/log/query [post]
func GetJobLogs(c *gin.Context) {
	var req ReqWithFilter
	if err := c.Bind(&req); err != nil {
		log.Error(jobLogStr, err)
		SendResponse(c, errno.ErrBind, nil)
		return
	}
	if req.Page == 0 {
		req.Page = 1
	}
	if req.PageSize == 0 {
		req.PageSize = 10
	}
	if req.Sort == "" {
		req.Sort = "start_time"
	}
	jobLogList, err := service.GetJobLogs(req.Name, req.Page, req.PageSize, req.Sort)
	if err != nil {
		SendResponse(c, errno.ErrGet, nil)
		return
	}
	SendResponse(c, errno.OK, jobLogList)
}
