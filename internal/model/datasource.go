package model

import (
	"database/sql/driver"
	"encoding/json"
	"strconv"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type DatabaseType string

const (
	DatabaseTypeMySQL      DatabaseType = "mysql"
	DatabaseTypeMariaDB    DatabaseType = "mariadb"
	DatabaseTypePostgreSQL DatabaseType = "postgresql"
	DatabaseTypeOracle     DatabaseType = "oracle"
)

type DataSourceStatus string

const (
	DataSourceStatusActive   DataSourceStatus = "active"
	DataSourceStatusInactive DataSourceStatus = "inactive"
	DataSourceStatusError    DataSourceStatus = "error"
)

// DataSource represents a database connection configuration
type DataSource struct {
	ID        string           `gorm:"type:char(36);primaryKey" json:"id"`
	Name      string           `gorm:"size:255;not null;uniqueIndex" json:"name"`
	Type      DatabaseType     `gorm:"type:enum('mysql','mariadb','postgresql','oracle');not null" json:"type"`
	Config    DataSourceConfig `gorm:"type:json;not null" json:"config"`
	Status    DataSourceStatus `gorm:"type:enum('active','inactive','error');default:'active'" json:"status"`
	CreatedAt time.Time        `json:"created_at"`
	UpdatedAt time.Time        `json:"updated_at"`
}

// DataSourceConfig holds the connection configuration for a data source
type DataSourceConfig struct {
	Host            string                 `json:"host" validate:"required"`
	Port            int                    `json:"port" validate:"required,min=1,max=65535"`
	Database        string                 `json:"database" validate:"required"`
	Username        string                 `json:"username" validate:"required"`
	Password        string                 `json:"password" validate:"required"` // This should be encrypted
	SSL             bool                  `json:"ssl"`
	Timeout         int                   `json:"timeout"`        // Connection timeout in seconds, default 30
	MaxPoolSize     int                   `json:"maxPoolSize"`    // Maximum pool size, default 10
	IdleTimeout     int                   `json:"idleTimeout"`    // Idle timeout in seconds, default 600
	MaxLifetime      int                   `json:"maxLifetime"`    // Max connection lifetime in seconds, default 1800
	Timezone         string                `json:"timezone"`       // Database timezone
	AdditionalProps  map[string]interface{} `json:"additionalProps,omitempty"`
}

// Value implements driver.Valuer interface for GORM
func (dsc DataSourceConfig) Value() (driver.Value, error) {
	return json.Marshal(dsc)
}

// Scan implements sql.Scanner interface for GORM
func (dsc *DataSourceConfig) Scan(value interface{}) error {
	if value == nil {
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return json.Unmarshal([]byte(v.(string)), dsc)
	}

	return json.Unmarshal(bytes, dsc)
}

// TableName returns the table name for the DataSource model
func (DataSource) TableName() string {
	return "data_sources"
}

// BeforeCreate generates a new UUID if ID is empty
func (ds *DataSource) BeforeCreate(tx *gorm.DB) error {
	if ds.ID == "" {
		ds.ID = uuid.New().String()
	}
	return nil
}

// GetConnectionURL returns the database connection URL for the specific database type
func (dsc *DataSourceConfig) GetConnectionURL(dbType DatabaseType) string {
	switch dbType {
	case DatabaseTypeMySQL, DatabaseTypeMariaDB:
		dsn := dsc.Username + ":" + dsc.Password + "@tcp(" + dsc.Host + ":" + strconv.Itoa(dsc.Port) + ")/" + dsc.Database
		if dsc.SSL {
			dsn += "?tls=true"
		}
		if dsc.Timezone != "" {
			if dsc.SSL {
				dsn += "&loc=" + dsc.Timezone
			} else {
				dsn += "?loc=" + dsc.Timezone
			}
		}
		return dsn

	case DatabaseTypePostgreSQL:
		dsn := "postgres://" + dsc.Username + ":" + dsc.Password + "@" + dsc.Host + ":" + strconv.Itoa(dsc.Port) + "/" + dsc.Database
		if dsc.SSL {
			dsn += "?sslmode=require"
		} else {
			dsn += "?sslmode=disable"
		}
		if dsc.Timezone != "" {
			dsn += "&TimeZone=" + dsc.Timezone
		}
		return dsn

	case DatabaseTypeOracle:
		return dsc.Username + "/" + dsc.Password + "@" + dsc.Host + ":" + strconv.Itoa(dsc.Port) + "/" + dsc.Database

	default:
		return ""
	}
}