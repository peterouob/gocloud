package service

import (
	"github.com/gin-gonic/gin"
	"github.com/peterouob/gocloud/db"
	"github.com/peterouob/gocloud/db/config"
	"github.com/peterouob/gocloud/db/sstable"
	s3bucket "github.com/peterouob/gocloud/s3"
	"net/http"
	"time"
)

func WriteData(c *gin.Context) {
	d := db.DB{}
	if err := c.ShouldBindJSON(&d); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	lsm := sstable.NewLSMTree[string, string]
	d.LsmTree = lsm(config.NewConfig(d.FileName))
	m := db.NewTableString(d.FileName, 10*time.Minute)
	if err := m.Put(d.Key, d.Value); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	err := d.LsmTree.FlushRecord(m, d.FileName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	c.JSON(http.StatusOK, gin.H{"data": d})
}

func ReadData(c *gin.Context) {
	d := db.DB{}
	if err := c.ShouldBindJSON(&d); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	lsm := sstable.NewLSMTree[string, string]
	d.LsmTree = lsm(config.NewConfig(d.FileName))
	data := string(d.LsmTree.Get(d.Key))
	if data == "" {
		c.JSON(http.StatusOK, gin.H{"data": nil})
	}
	c.JSON(http.StatusOK, gin.H{"data": data})
}

func UploadToBucket(c *gin.Context) {
	s3file := s3bucket.S3File{}
	if err := c.ShouldBindJSON(&s3file); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	client := s3bucket.NewClient(c)
	s3file.Client = client
	if err := s3file.UploadFile(c); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": s3file})
}

func ReadFile(c *gin.Context) {
	s3file := &s3bucket.S3File{}
	key := c.Param("key")
	s3file.Key = key + ".txt"
	client := s3bucket.NewClient(c)
	s3file.Client = client
	data, err := s3file.ReadS3File(c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data": data,
	})
}
