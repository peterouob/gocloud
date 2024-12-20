package main

import (
	"github.com/gin-gonic/gin"
	"github.com/peterouob/gocloud/router"
)

func main() {
	r := gin.Default()
	router.SetupRouter(r)
	r.Run(":8083")
}
