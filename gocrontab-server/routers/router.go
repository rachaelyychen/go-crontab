//	routers包用于处理路由分发逻辑，根据路由定位到handler层相关方法
package routers

import (
	"github.com/gin-gonic/gin"
	"gocrontab-server/handler"
	"net/http"
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

func Load(g *gin.Engine) *gin.Engine {
	g.NoRoute(func(c *gin.Context) {
		c.String(http.StatusNotFound, "Incorrect API route.")
	})

	// static file
	g.Static("/webroot", "./resource/webroot")
	g.LoadHTMLFiles("./resource/webroot/index.html")

	m := g.Group("/crontab/job")
	m.POST("", handler.CreateJob)
	m.POST("/query", handler.GetJobs)
	m.DELETE("", handler.DeleteJob)    // form data
	m.POST("/killer", handler.KillJob)
	m.POST("/log/query", handler.GetJobLogs)

	m = g.Group("/crontab/worker")
	m.POST("/query", handler.GetHWorkers)

	return g
}
