package model

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// AuthType represents the authentication method type
type AuthType string

const (
	AuthTypeBasic     AuthType = "basic"
	AuthTypeOAuth2    AuthType = "oauth2"
	AuthTypeIAM       AuthType = "iam"
	AuthTypeKerberos  AuthType = "kerberos"
	AuthTypeJWT       AuthType = "jwt"
	AuthTypeSAS       AuthType = "sas"
	AuthTypePAT       AuthType = "pat"
	AuthTypeKeyPair   AuthType = "keypair"
)

// CredentialVault represents encrypted credential storage
type CredentialVault struct {
	ID                   string    `gorm:"type:char(36);primaryKey" json:"id"`
	DataSourceID         string    `gorm:"type:char(36);uniqueIndex;not null" json:"data_source_id"`
	EncryptedCredentials string    `gorm:"type:text;not null" json:"-"` // Never expose in JSON
	EncryptionKeyID      string    `gorm:"type:varchar(100)" json:"encryption_key_id,omitempty"`
	AuthType             AuthType  `gorm:"type:enum('basic','oauth2','iam','kerberos','jwt','sas','pat','keypair');not null" json:"auth_type"`
	TokenExpiresAt      *time.Time `json:"token_expires_at,omitempty"`
	LastRotatedAt        *time.Time `json:"last_rotated_at,omitempty"`
	CreatedAt            time.Time `gorm:"not null;default:CURRENT_TIMESTAMP" json:"created_at"`
	UpdatedAt            time.Time `gorm:"not null;default:CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP" json:"updated_at"`

	// Relationships
	DataSource *DataSource `gorm:"foreignKey:DataSourceID" json:"-"`
}

// TableName returns the table name for CredentialVault
func (CredentialVault) TableName() string {
	return "credential_vault"
}

// BeforeCreate generates a new UUID if ID is empty
func (cv *CredentialVault) BeforeCreate(tx *gorm.DB) error {
	if cv.ID == "" {
		cv.ID = uuid.New().String()
	}
	return nil
}

// NeedsRotation checks if the credentials need to be rotated
func (cv *CredentialVault) NeedsRotation() bool {
	if cv.TokenExpiresAt == nil {
		return false
	}
	// Rotate if token expires within 5 minutes
	return time.Until(*cv.TokenExpiresAt) < 5*time.Minute
}

// Credentials represents the decrypted credential structure (in-memory only)
type Credentials struct {
	Username           string                 `json:"username,omitempty"`
	Password           string                 `json:"password,omitempty"`
	AccessToken        string                 `json:"access_token,omitempty"`
	RefreshToken       string                 `json:"refresh_token,omitempty"`
	PrivateKey         string                 `json:"private_key,omitempty"`
	Keytab             string                 `json:"keytab,omitempty"` // base64-encoded
	CredentialsJSON    string                 `json:"credentials_json,omitempty"` // service account JSON
	IAMRoleArn         string                 `json:"iam_role_arn,omitempty"`
	SASToken           string                 `json:"sas_token,omitempty"`
	PATToken           string                 `json:"pat_token,omitempty"`
	AdditionalSecrets  map[string]interface{} `json:"additional_secrets,omitempty"`
}
