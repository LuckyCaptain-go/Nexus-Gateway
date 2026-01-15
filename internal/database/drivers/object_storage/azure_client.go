package object_storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// AzureBlobClient wraps Azure Blob Storage client
type AzureBlobClient struct {
	client    *azblob.Client
	container string
	config    *AzureBlobConfig
}

// AzureBlobConfig holds Azure Blob Storage configuration
type AzureBlobConfig struct {
	AccountName   string
	AccountKey    string
	SASToken      string // Shared Access Signature token
	ContainerName string
	Endpoint      string // Optional custom endpoint
}

// NewAzureBlobClient creates a new Azure Blob Storage client
func NewAzureBlobClient(ctx context.Context, config *AzureBlobConfig) (*AzureBlobClient, error) {
	if config.AccountName == "" {
		return nil, fmt.Errorf("account name is required")
	}

	// Build blob service URL
	blobURL := fmt.Sprintf("https://%s.blob.core.windows.net/", config.AccountName)
	if config.Endpoint != "" {
		blobURL = config.Endpoint
	}

	// Create client
	var client *azblob.Client
	var err error

	if config.SASToken != "" {
		// Use SAS token
		client, err = azblob.NewClientWithNoCredential(blobURL, &azblob.ClientOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob client with SAS: %w", err)
		}
	} else {
		// Use shared key
		credential, err := azblob.NewSharedKeyCredential(config.AccountName, config.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create shared key credential: %w", err)
		}

		client, err = azblob.NewClientWithSharedKeyCredential(blobURL, credential, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob client: %w", err)
		}
	}

	return &AzureBlobClient{
		client:    client,
		container: config.ContainerName,
		config:    config,
	}, nil
}

// ListBlobs lists blobs in a container
func (c *AzureBlobClient) ListBlobs(ctx context.Context, prefix string, maxResults int) ([]AzureBlob, error) {
	maxResults32 := int32(maxResults)
	pager := c.client.NewListBlobsFlatPager(c.container, &azblob.ListBlobsFlatOptions{
		Prefix:     &prefix,
		MaxResults: &maxResults32,
	})

	var blobs []AzureBlob

	page, err := pager.NextPage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list blobs: %w", err)
	}

	for _, blob := range page.Segment.BlobItems {
		blobs = append(blobs, AzureBlob{
			Name:         *blob.Name,
			Size:         *blob.Properties.ContentLength,
			LastModified: *blob.Properties.LastModified,
			ETag:         string(*blob.Properties.ETag),
			ContentType:  *blob.Properties.ContentType,
		})
	}

	return blobs, nil
}

// GetBlob retrieves a blob from Azure Blob Storage
func (c *AzureBlobClient) GetBlob(ctx context.Context, name string) ([]byte, error) {
	resp, err := c.client.DownloadStream(ctx, c.container, name, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to download blob: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read blob data: %w", err)
	}

	return data, nil
}

// GetBlobMetadata retrieves blob metadata
func (c *AzureBlobClient) GetBlobMetadata(ctx context.Context, name string) (*AzureBlobMetadata, error) {
	maxResults := int32(1)
	pager := c.client.NewListBlobsFlatPager(c.container, &azblob.ListBlobsFlatOptions{
		Prefix:     &name,
		MaxResults: &maxResults,
	})

	page, err := pager.NextPage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list blobs for metadata: %w", err)
	}

	for _, blob := range page.Segment.BlobItems {
		if *blob.Name == name {
			metadata := &AzureBlobMetadata{
				Name:          name,
				ContentLength: *blob.Properties.ContentLength,
				ContentType:   *blob.Properties.ContentType,
				LastModified:  *blob.Properties.LastModified,
				ETag:          string(*blob.Properties.ETag),
				BlobType:      string(*blob.Properties.BlobType),
			}

			if blob.Properties.ContentEncoding != nil {
				metadata.ContentEncoding = *blob.Properties.ContentEncoding
			}

			if blob.Properties.CacheControl != nil {
				metadata.CacheControl = *blob.Properties.CacheControl
			}

			return metadata, nil
		}
	}

	return nil, fmt.Errorf("blob not found: %s", name)
}

// PutBlob uploads a blob to Azure Blob Storage
func (c *AzureBlobClient) PutBlob(ctx context.Context, name string, data []byte, contentType string) error {
	_, err := c.client.UploadBuffer(ctx, c.container, name, data, nil)
	if err != nil {
		return fmt.Errorf("failed to upload blob: %w", err)
	}

	return nil
}

// DeleteBlob deletes a blob from Azure Blob Storage
func (c *AzureBlobClient) DeleteBlob(ctx context.Context, name string) error {
	_, err := c.client.DeleteBlob(ctx, c.container, name, nil)
	if err != nil {
		return fmt.Errorf("failed to delete blob: %w", err)
	}

	return nil
}

// CopyBlob copies a blob within Azure Blob Storage
func (c *AzureBlobClient) CopyBlob(ctx context.Context, srcName, destName string) error {
	// First get the source blob content
	data, err := c.GetBlob(ctx, srcName)
	if err != nil {
		return fmt.Errorf("failed to get source blob: %w", err)
	}

	// Get source blob metadata to preserve content type
	metadata, err := c.GetBlobMetadata(ctx, srcName)
	if err != nil {
		return fmt.Errorf("failed to get source blob metadata: %w", err)
	}

	// Upload the content to the destination blob
	return c.PutBlob(ctx, destName, data, metadata.ContentType)
}

// GenerateSASToken generates a SAS token for blob access
func (c *AzureBlobClient) GenerateSASToken(ctx context.Context, name string, expiration time.Duration) (string, error) {
	// Simplified SAS token generation
	// Full implementation would use Azure SAS builder
	return "", fmt.Errorf("SAS token generation not yet implemented")
}

// BlobExists checks if a blob exists
func (c *AzureBlobClient) BlobExists(ctx context.Context, name string) (bool, error) {
	maxResults := int32(1)
	pager := c.client.NewListBlobsFlatPager(c.container, &azblob.ListBlobsFlatOptions{
		Prefix:     &name,
		MaxResults: &maxResults,
	})

	page, err := pager.NextPage(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to list blobs: %w", err)
	}

	for _, blob := range page.Segment.BlobItems {
		if *blob.Name == name {
			return true, nil
		}
	}

	return false, nil
}

// GetBlobSize gets the size of a blob
func (c *AzureBlobClient) GetBlobSize(ctx context.Context, name string) (int64, error) {
	metadata, err := c.GetBlobMetadata(ctx, name)
	if err != nil {
		return 0, err
	}
	return metadata.ContentLength, nil
}

// AzureBlob represents an Azure blob
type AzureBlob struct {
	Name         string
	Size         int64
	LastModified time.Time
	ETag         string
	ContentType  string
}

// AzureBlobMetadata represents Azure blob metadata
type AzureBlobMetadata struct {
	Name            string
	ContentLength   int64
	ContentType     string
	ContentEncoding string
	LastModified    time.Time
	ETag            string
	CacheControl    string
	BlobType        string
}

// ListBlobsByExtension lists blobs with a specific extension
func (c *AzureBlobClient) ListBlobsByExtension(ctx context.Context, prefix, extension string) ([]AzureBlob, error) {
	blobs, err := c.ListBlobs(ctx, prefix, 5000)
	if err != nil {
		return nil, err
	}

	var filtered []AzureBlob
	for _, blob := range blobs {
		if hasExtension(blob.Name, extension) {
			filtered = append(filtered, blob)
		}
	}

	return filtered, nil
}

// GetContainerInfo returns container information
func (c *AzureBlobClient) GetContainerInfo(ctx context.Context) *AzureContainerInfo {
	return &AzureContainerInfo{
		Name:        c.container,
		AccountName: c.config.AccountName,
	}
}

// AzureContainerInfo represents Azure container information
type AzureContainerInfo struct {
	Name        string
	AccountName string
	Endpoint    string
}
