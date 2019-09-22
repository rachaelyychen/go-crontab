//	handler包用于处理controller层相关
package handler

import (
	"github.com/gin-gonic/gin"
	"gocrontab-server/pkg/errno"
	"net/http"
)

/**
* @project: gocrontab
*
* @description:
*
* @author: cyy
*
* @create: 8/31/19 12:11 PM
**/

type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

type ReqWithFilter struct {
	Name     string `json:"name"`
	Page     int64  `json:"page"`
	PageSize int64  `json:"page_size"`
	Sort     string `json:"sort"`
}

func SendResponse(c *gin.Context, err *errno.Errno, data interface{}) {
	c.JSON(http.StatusOK, Response{
		Code:    err.Code,
		Message: err.Message,
		Data:    data,
	})
}
