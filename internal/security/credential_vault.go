package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
)

var (
	ErrInvalidCiphertext = errors.New("invalid ciphertext")
	ErrInvalidKeyLength  = errors.New("encryption key must be 32 bytes for AES-256")
)

// CredentialVault handles encryption and decryption of credentials
type CredentialVault struct {
	masterKey []byte
}

// NewCredentialVault creates a new credential vault with the given master key
// The master key should be 32 bytes for AES-256-GCM
func NewCredentialVault(masterKey []byte) (*CredentialVault, error) {
	if len(masterKey) != 32 {
		return nil, ErrInvalidKeyLength
	}
	return &CredentialVault{masterKey: masterKey}, nil
}

// EncryptCredentials encrypts the credentials using AES-256-GCM
// Returns base64-encoded ciphertext
func (cv *CredentialVault) EncryptCredentials(plaintext []byte) (string, error) {
	block, err := aes.NewCipher(cv.masterKey)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}

	// Generate a random nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt and authenticate
	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)

	// Return base64-encoded (nonce || ciphertext)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// DecryptCredentials decrypts the base64-encoded ciphertext
func (cv *CredentialVault) DecryptCredentials(ciphertextB64 string) ([]byte, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return "", fmt.Errorf("failed to decode base64: %w", err)
	}

	block, err := aes.NewCipher(cv.masterKey)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return "", ErrInvalidCiphertext
	}

	nonce, cipherText := ciphertext[:nonceSize], ciphertext[nonceSize:]

	plaintext, err := gcm.Open(nil, nonce, cipherText, nil)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt: %w", err)
	}

	return plaintext, nil
}
