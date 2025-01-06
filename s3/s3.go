package s3bucket

import (
	"bufio"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"log"
	"os"
	"unicode"
)

type S3File struct {
	File   string `json:"file"`
	Key    string `json:"key"`
	Client *s3.Client
}

var (
	awsRegion = "us-east-1"
	bucket    = "s3-mdb-bucket"
)

func NewClient(ctx context.Context) *s3.Client {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(awsRegion))
	if err != nil {
		panic(err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("https://localhost.localstack.cloud:4566")
	})

	return client
}

func (s3file *S3File) UploadFile(ctx context.Context) error {

	if err := readFileAndFilterString(s3file.File, s3file.Key); err != nil {
		return fmt.Errorf("readFileAndFilterString: %w", err)
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s3file.Key),
	}

	log.Printf("PutObjectInput: %+v", input)

	if _, err := s3file.Client.PutObject(ctx, input); err != nil {
		return fmt.Errorf("failed to upload file: %w", err)

	}
	return nil
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

func readFileAndFilterString(inputFile, outputFile string) error {
	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	outFile, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	scanner := bufio.NewScanner(file)
	writer := bufio.NewWriter(outFile)

	for scanner.Scan() {
		line := scanner.Text()
		filtered := filterString(line)
		_, err = writer.WriteString(filtered + "\n")
		if err != nil {
			return fmt.Errorf("failed to write to output file: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error while reading file: %w", err)
	}

	return nil
}

func filterString(input string) string {
	filtered := []rune{}
	for _, r := range input {
		if unicode.IsPrint(r) {
			filtered = append(filtered, r)
		}
	}
	return string(filtered)
}
