package object_storage

import (
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
)

// AzureSASTokenHandler handles SAS token generation and management
type AzureSASTokenHandler struct {
	accountName   string
	accountKey    string
	containerName string
}

// NewAzureSASTokenHandler creates a new SAS token handler
func NewAzureSASTokenHandler(accountName, accountKey, containerName string) *AzureSASTokenHandler {
	return &AzureSASTokenHandler{
		accountName:   accountName,
		accountKey:    accountKey,
		containerName: containerName,
	}
}

// GenerateSASToken generates a SAS token for blob access
func (h *AzureSASTokenHandler) GenerateSASToken(blobName string, permissions string, expiry time.Duration) (string, error) {
	if h.accountName == "" || h.accountKey == "" {
		return "", fmt.Errorf("account name and key are required")
	}

	// Parse expiry time
	expiryTime := time.Now().Add(expiry)

	// Create SAS service
	credential, err := azblob.NewSharedKeyCredential(h.accountName, h.accountKey)
	if err != nil {
		return "", fmt.Errorf("failed to create credential: %w", err)
	}

	// Build SAS URL
	sasQueryParams, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     time.Now().UTC().Add(-5 * time.Minute),
		ExpiryTime:    expiryTime.UTC(),
		ContainerName: h.containerName,
		BlobName:      blobName,
		Permissions:   sas.BlobPermissions(permissions).String(),
	}.SignWithSharedKey(credential)
	if err != nil {
		return "", fmt.Errorf("failed to sign SAS token: %w", err)
	}

	sasToken := sasQueryParams.Encode()
	return sasToken, nil
}

// GenerateContainerSAS generates a SAS token for entire container
func (h *AzureSASTokenHandler) GenerateContainerSAS(permissions string, expiry time.Duration) (string, error) {
	if h.accountName == "" || h.accountKey == "" {
		return "", fmt.Errorf("account name and key are required")
	}

	expiryTime := time.Now().Add(expiry)

	credential, err := azblob.NewSharedKeyCredential(h.accountName, h.accountKey)
	if err != nil {
		return "", fmt.Errorf("failed to create credential: %w", err)
	}

	sasQueryParams, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     time.Now().UTC().Add(-5 * time.Minute),
		ExpiryTime:    expiryTime.UTC(),
		ContainerName: h.containerName,
		Permissions:   sas.BlobPermissions(permissions).String(),
	}.SignWithSharedKey(credential)
	if err != nil {
		return "", fmt.Errorf("failed to sign SAS token: %w", err)
	}

	sasToken := sasQueryParams.Encode()
	return sasToken, nil
}

// ParseSASToken parses an existing SAS token
func (h *AzureSASTokenHandler) ParseSASToken(sasToken string) (*SASTokenInfo, error) {
	if sasToken == "" {
		return nil, fmt.Errorf("SAS token cannot be empty")
	}

	// Remove leading '?' if present
	sasToken = strings.TrimPrefix(sasToken, "?")

	pairs := strings.Split(sasToken, "&")
	info := &SASTokenInfo{}

	for _, pair := range pairs {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			continue
		}

		key := kv[0]
		value := kv[1]

		switch key {
		case "sig":
			info.Signature = value
		case "se":
			expiry, err := time.Parse("2006-01-02T15:04:05Z", value)
			if err == nil {
				info.ExpiryTime = expiry
			}
		case "st":
			startTime, err := time.Parse("2006-01-02T15:04:05Z", value)
			if err == nil {
				info.StartTime = startTime
			}
		case "sp":
			info.Permissions = value
		case "spr":
			info.Protocol = value
		}
	}

	return info, nil
}

// IsSASTokenValid checks if a SAS token is still valid
func (h *AzureSASTokenHandler) IsSASTokenValid(sasToken string) bool {
	info, err := h.ParseSASToken(sasToken)
	if err != nil {
		return false
	}

	// Check if expired
	if !info.ExpiryTime.IsZero() && time.Now().UTC().After(info.ExpiryTime) {
		return false
	}

	// Check if not yet valid
	if !info.StartTime.IsZero() && time.Now().UTC().Before(info.StartTime) {
		return false
	}

	return true
}

// GetBlobURLWithSAS returns the blob URL with SAS token appended
func (h *AzureSASTokenHandler) GetBlobURLWithSAS(blobName string, sasToken string) string {
	baseURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s",
		h.accountName, h.containerName, blobName)

	if sasToken != "" {
		if !strings.HasPrefix(sasToken, "?") {
			baseURL += "?"
		}
		baseURL += sasToken
	}

	return baseURL
}

// SASTokenInfo represents parsed SAS token information
type SASTokenInfo struct {
	Signature   string
	StartTime   time.Time
	ExpiryTime  time.Time
	Permissions string
	Protocol    string
}

// GetExpiryStatus returns the expiry status of a SAS token
func (i *SASTokenInfo) GetExpiryStatus() SASTokenStatus {
	now := time.Now().UTC()

	if i.ExpiryTime.IsZero() {
		return SASTokenStatusUnknown
	}

	if now.After(i.ExpiryTime) {
		return SASTokenStatusExpired
	}

	if i.ExpiryTime.Sub(now) < 5*time.Minute {
		return SASTokenStatusExpiringSoon
	}

	return SASTokenStatusValid
}

// SASTokenStatus represents SAS token validity status
type SASTokenStatus int

const (
	SASTokenStatusValid SASTokenStatus = iota
	SASTokenStatusExpiringSoon
	SASTokenStatusExpired
	SASTokenStatusUnknown
)

func (s SASTokenStatus) String() string {
	switch s {
	case SASTokenStatusValid:
		return "valid"
	case SASTokenStatusExpiringSoon:
		return "expiring_soon"
	case SASTokenStatusExpired:
		return "expired"
	default:
		return "unknown"
	}
}

// GenerateReadOnlySAS generates a read-only SAS token
func (h *AzureSASTokenHandler) GenerateReadOnlySAS(blobName string, expiry time.Duration) (string, error) {
	return h.GenerateSASToken(blobName, "r", expiry)
}

// GenerateReadWriteSAS generates a read-write SAS token
func (h *AzureSASTokenHandler) GenerateReadWriteSAS(blobName string, expiry time.Duration) (string, error) {
	return h.GenerateSASToken(blobName, "rw", expiry)
}

// GenerateListSAS generates a list-only SAS token
func (h *AzureSASTokenHandler) GenerateListSAS(expiry time.Duration) (string, error) {
	return h.GenerateContainerSAS("l", expiry)
}
