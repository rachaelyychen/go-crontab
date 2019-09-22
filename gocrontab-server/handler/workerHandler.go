package handler

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"gocrontab-server/pkg/errno"
	"gocrontab-server/service"
	"gocrontab-server/utils"
)

/**
* @project: gocrontab-server
*
* @description:
*
* @author: cyy
*
* @create: 9/13/19 3:24 PM
**/

var workerLogStr = fmt.Sprintf(utils.LOGSTR, "crontab-master", "workerHandler")

// @Summary 获取健康节点
// @Accept
// @Produce  json
// @Success 200 {} json "{}"
// @Router /crontab/worker/query [post]
func GetHWorkers(c *gin.Context) {
	workerList, err := service.GetHWorkers()
	if err != nil {
		SendResponse(c, errno.ErrGet, nil)
		return
	}
	SendResponse(c, errno.OK, workerList)
}
