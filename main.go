package main

import (
	"bytes"
	"log"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	s3_region             = "us-east-1"
	s3_bucket             = "hcp-<YOUR-S3-BUCKET>"
	aws_access_key_id     = "<YOUR-AWS-ACCESS-KEY-ID>"
	aws_secret_access_key = "<YOUT-AWS-SECRET-ACCESS-KEY>"
)

func main() {

	client := getS3Client()

	key := "<YOUR-FILE-KEY>"
	fileName := "Test.pdf"

	uResp, err := uploadFileToS3(client, key, fileName)
	if err != nil {
		log.Fatalf("failed to upload file - %v", err)
	}
	log.Printf("uploaded file with response %s", uResp)

	dResp, err := downloadFileFromS3(client, key)
	if err != nil {
		log.Fatalf("failed to download file - %v", err)
	}
	log.Printf("downloaded file %s", dResp)

	listFilesInS3(client)
}

func uploadFileToS3(s3Client *s3.S3, key string, fileName string) (string, error) {
	filePath := "./static/" + fileName
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open the file - %v", err)
		return "", err
	}
	defer file.Close()

	fileInfo, _ := file.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)

	file.Read(buffer)
	reader := bytes.NewReader(buffer)
	fileType := http.DetectContentType(buffer)
	path := "/media/" + key // or it can also be file.Name()
	params := &s3.PutObjectInput{
		Bucket:        aws.String(s3_bucket),
		Key:           aws.String(path),
		Body:          reader,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String(fileType),
	}

	resp, err := s3Client.PutObject(params)
	if err != nil {
		log.Fatalf("failed to put the file into the bucket - %v", err)
		return "", err
	}

	return awsutil.StringValue(resp), nil
}

func downloadFileFromS3(s3Client *s3.S3, key string) (string, error) {
	cfg := s3Client.Config
	path := "/media/" + key
	sess, _ := session.NewSession(&cfg)

	file, err := os.Create("./static/" + "downloaded_pdf.pdf")
	if err != nil {
		log.Fatalf("Unable to create a file to download into %v", err)
	}

	defer file.Close()

	params := &s3.GetObjectInput{
		Bucket: aws.String(s3_bucket),
		Key:    aws.String(path),
	}

	downloader := s3manager.NewDownloader(sess)

	_, err = downloader.Download(file, params)
	if err != nil {
		log.Fatalf("failed to get the file from the bucket - %v", err)
		return "", err
	}

	return file.Name(), nil
}

func listFilesInS3(s3Client *s3.S3) {
	var i int64

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(s3_bucket),
	}

	resp, err := s3Client.ListObjectsV2(params)
	if err != nil {
		log.Fatalf("failed to list objects in the bucket - %v", err)
	}

	for i = 0; i < aws.Int64Value(resp.KeyCount); i++ {
		objContents := resp.Contents[i].String()
		log.Println(objContents)
	}
}

func getS3Client() *s3.S3 {
	token := ""
	creds := credentials.NewStaticCredentials(aws_access_key_id, aws_secret_access_key, token)
	_, err := creds.Get()
	if err != nil {
		log.Fatalf("failed to use credentials %v", err)
	}

	cfg := aws.NewConfig().WithRegion(s3_region).WithCredentials(creds)
	svc := s3.New(session.New(), cfg)
	return svc
}
