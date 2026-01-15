package object_storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"bytes"
	"net/http"

	"github.com/tencentyun/cos-go-sdk-v5"
)

// COSClient wraps Tencent Cloud COS client
type COSClient struct {
	client *cos.Client
	bucket string
	config *COSConfig
}

// COSConfig holds Tencent Cloud COS configuration
type COSConfig struct {
	SecretID   string
	SecretKey  string
	Region     string // e.g., ap-guangzhou
	BucketName string
	HTTPS      bool
}

// NewCOSClient creates a new Tencent Cloud COS client
func NewCOSClient(ctx context.Context, config *COSConfig) (*COSClient, error) {
	if config.SecretID == "" {
		return nil, fmt.Errorf("secret ID is required")
	}
	if config.SecretKey == "" {
		return nil, fmt.Errorf("secret key is required")
	}
	if config.Region == "" {
		return nil, fmt.Errorf("region is required")
	}

	// Build COS bucket URL
	bucketURL, err := cos.NewBucketURL(config.BucketName, config.Region, config.HTTPS)
	if err != nil {
		return nil, fmt.Errorf("failed to create bucket URL: %w", err)
	}

	// Create COS client
	client := cos.NewClient(&cos.BaseURL{BucketURL: bucketURL}, &http.Client{
		Timeout: 30 * time.Second,
	})

	return &COSClient{
		client: client,
		bucket: config.BucketName,
		config: config,
	}, nil
}

// ListObjects lists objects in a bucket
func (c *COSClient) ListObjects(ctx context.Context, prefix string, maxKeys int) ([]COSObject, error) {
	listOpts := &cos.BucketGetOptions{
		Prefix:  prefix,
		MaxKeys: maxKeys,
	}

	resp, _, err := c.client.Bucket.Get(ctx, listOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	objects := make([]COSObject, 0, len(resp.Contents))
	for _, obj := range resp.Contents {
				parsedTime, _ := time.Parse(time.RFC3339, obj.LastModified)
			objects = append(objects, COSObject{
				Key:          obj.Key,
				Size:         int64(obj.Size),
				LastModified: parsedTime,
				ETag:         obj.ETag,
			})
	}

	return objects, nil
}

// GetObject retrieves an object from COS
func (c *COSClient) GetObject(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.client.Object.Get(ctx, key, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object data: %w", err)
	}

	return data, nil
}

// GetObjectMetadata retrieves object metadata
func (c *COSClient) GetObjectMetadata(ctx context.Context, key string) (*COSObjectMetadata, error) {
	resp, err := c.client.Object.Head(ctx, key, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to head object: %w", err)
	}

	metadata := &COSObjectMetadata{
		Key:           key,
		ContentLength: int64(resp.ContentLength),
		ContentType:   resp.Header.Get("Content-Type"),
		ETag:          resp.Header.Get("ETag"),
	}

	if lastModified := resp.Header.Get("Last-Modified"); lastModified != "" {
		parsedTime, _ := time.Parse(time.RFC1123, lastModified)
		metadata.LastModified = parsedTime
	}

	return metadata, nil
}

// PutObject uploads an object to COS
func (c *COSClient) PutObject(ctx context.Context, key string, data []byte, contentType string) error {
	opts := &cos.ObjectPutOptions{}
	if contentType != "" {
		opts.ObjectPutHeaderOptions = &cos.ObjectPutHeaderOptions{
			ContentType: contentType,
		}
	}

	_, err := c.client.Object.Put(ctx, key, bytes.NewReader(data), opts)
	if err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}

	return nil
}

// DeleteObject deletes an object from COS
func (c *COSClient) DeleteObject(ctx context.Context, key string) error {
	_, err := c.client.Object.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}

	return nil
}

// CopyObject copies an object within COS
func (c *COSClient) CopyObject(ctx context.Context, srcKey, destKey string) error {
	sourceURL := fmt.Sprintf("%s.cos.ap-%s.myqcloud.com/%s", c.bucket, c.config.Region, srcKey)

	_, _, err := c.client.Object.Copy(ctx, destKey, sourceURL, nil)
	if err != nil {
		return fmt.Errorf("failed to copy object: %w", err)
	}

	return nil
}

// PresignURL generates a presigned URL for temporary access
func (c *COSClient) PresignURL(ctx context.Context, key string, expiration time.Duration) (string, error) {
	presignedURL, err := c.client.Object.GetPresignedURL(ctx, http.MethodGet, key, c.config.SecretID, c.config.SecretKey, expiration, nil)
	if err != nil {
		return "", fmt.Errorf("failed to presign URL: %w", err)
	}

	return presignedURL.String(), nil
}

// ObjectExists checks if an object exists
func (c *COSClient) ObjectExists(ctx context.Context, key string) (bool, error) {
	_, err := c.client.Object.Head(ctx, key, nil)
	if err != nil {
		// Check if error is "not found"
		if cos.IsNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// GetObjectSize gets the size of an object
func (c *COSClient) GetObjectSize(ctx context.Context, key string) (int64, error) {
	metadata, err := c.GetObjectMetadata(ctx, key)
	if err != nil {
		return 0, err
	}
	return metadata.ContentLength, nil
}

// COSObject represents a COS object
type COSObject struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
}

// COSObjectMetadata represents COS object metadata
type COSObjectMetadata struct {
	Key             string
	ContentLength   int64
	ContentType     string
	ContentEncoding string
	LastModified    time.Time
	ETag            string
	CacheControl    string
}

// ListFilesByExtension lists files with a specific extension
func (c *COSClient) ListFilesByExtension(ctx context.Context, prefix, extension string) ([]COSObject, error) {
	objects, err := c.ListObjects(ctx, prefix, 1000)
	if err != nil {
		return nil, err
	}

	var filtered []COSObject
	for _, obj := range objects {
		if hasExtension(obj.Key, extension) {
			filtered = append(filtered, obj)
		}
	}

	return filtered, nil
}

// GetBucketInfo returns bucket information
func (c *COSClient) GetBucketInfo(ctx context.Context) *COSBucketInfo {
	protocol := "https"
	if c.config.HTTPS {
		protocol = "https"
	}

	return &COSBucketInfo{
		Name:   c.bucket,
		Region: c.config.Region,
		URL:    fmt.Sprintf("%s://%s.cos.ap-%s.myqcloud.com", protocol, c.bucket, c.config.Region),
	}
}

// COSBucketInfo represents COS bucket information
type COSBucketInfo struct {
	Name   string
	Region string
	URL    string
}
