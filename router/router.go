package router

import (
	"github.com/gin-gonic/gin"
	"github.com/peterouob/gocloud/service"
)

func SetupRouter(r *gin.Engine) {
	r.POST("/", service.WriteData)
	r.PUT("/upload", service.UploadToBucket)
	r.GET("/file/:key", service.ReadFile)
}
