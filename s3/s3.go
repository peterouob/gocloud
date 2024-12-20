package s3bucket

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"os"
	"strings"
)

type S3File struct {
	File   string `json:"file"`
	Key    string `json:"key"`
	Client *s3.Client
}

const (
	awsEndpoint = "http://localhost:4566"
	awsRegion   = "us-east-1"
	bucket      = "s3-demo-bucket"
)

func NewClient(ctx context.Context) *s3.Client {

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(awsRegion))
	if err != nil {
		panic(err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(awsEndpoint)
		o.UsePathStyle = true
	})

	return client
}

func (s3file *S3File) UploadFile(ctx context.Context) error {
	content, err := ReadLocalFile(s3file.File)
	if err != nil {
		return errors.New("error in reading local file : " + err.Error())
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s3file.Key),
		Body:   strings.NewReader(content),
	}

	_, err = s3file.Client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}
	return nil
}

func ReadLocalFile(filepath string) (string, error) {
	content, err := os.ReadFile(filepath)
	if err != nil {
		return "", fmt.Errorf("failed to read local file: %w", err)
	}
	return string(content), nil
}

func (s3file *S3File) ReadS3File(ctx context.Context) (string, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s3file.Key),
	}

	resp, err := s3file.Client.GetObject(ctx, input)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read object body: %w", err)
	}

	return string(content), nil
}
