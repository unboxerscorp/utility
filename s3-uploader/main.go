package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/text/unicode/norm"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run main.go '<local-folder>' '<s3-path>'")
		fmt.Println("Example: go run main.go './공수 1강' 'base-inbrain-resource/lectures/'")
		os.Exit(1)
	}

	localFolder := os.Args[1]
	s3Path := os.Args[2]

	// Parse S3 path (bucket/prefix)
	parts := strings.SplitN(s3Path, "/", 2)
	if len(parts) < 1 {
		log.Fatal("Invalid S3 path format. Expected: base-inbrain-resource/lectures/")
	}

	bucket := parts[0]
	prefix := ""
	if len(parts) > 1 {
		prefix = parts[1]
	}

	// Initialize AWS config
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg)

	// Walk through local folder recursively
	err = filepath.Walk(localFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Get relative path from local folder
		relPath, err := filepath.Rel(localFolder, path)
		if err != nil {
			return err
		}

		// Get folder name and include it in the path
		folderName := filepath.Base(localFolder)
		
		// Convert path separators to forward slashes for S3
		s3Key := filepath.ToSlash(filepath.Join(folderName, relPath))

		// Convert NFD to NFC
		s3Key = norm.NFC.String(s3Key)

		// Add prefix if provided
		if prefix != "" {
			s3Key = prefix + s3Key
		}

		// Upload file to S3
		fmt.Printf("Uploading %s to s3://%s/%s\n", path, bucket, s3Key)

		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %v", path, err)
		}
		defer func() {
			_ = file.Close()
		}()

		_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(s3Key),
			Body:   file,
		})
		if err != nil {
			return fmt.Errorf("failed to upload %s: %v", path, err)
		}

		fmt.Printf("Successfully uploaded %s\n", s3Key)
		return nil
	})

	if err != nil {
		log.Fatalf("Error walking directory: %v", err)
	}

	fmt.Println("Upload completed successfully!")
}
