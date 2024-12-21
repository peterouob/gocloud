package router

import (
	"github.com/gin-gonic/gin"
	"github.com/peterouob/gocloud/service"
)

func SetupRouter(r *gin.Engine) {
	r.POST("/", service.WriteData)
	r.PUT("/upload", service.UploadToBucket)
	r.GET("/file/:key", service.ReadFile)
	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "hello world",
		})
	})
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

}
