package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Database  DatabaseConfig  `yaml:"database"`
	Security  SecurityConfig  `yaml:"security"`
	Logging   LoggingConfig   `yaml:"logging"`
	Query     QueryConfig     `yaml:"query"`
	MCP       MCPConfig       `yaml:"mcp"`  // Added MCP configuration
}

type ServerConfig struct {
	Port string `yaml:"port"`
	Mode string `yaml:"mode"`  // debug, release, test
	Host string `yaml:"host"`
}

type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Database string `yaml:"database"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	SSL      string `yaml:"ssl"`
}

type SecurityConfig struct {
	JWTSecret            string `yaml:"jwt_secret"`
	JWTExpiration        string `yaml:"jwt_expiration"`
	RateLimitPerMinute   int    `yaml:"rate_limit_per_minute"`
	RateLimitBurst       int    `yaml:"rate_limit_burst"`
	EnableAuth           bool   `yaml:"enable_auth"`
	EnableRateLimit      bool   `yaml:"enable_rate_limit"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`  // debug, info, warn, error
	Format string `yaml:"format"` // json, text
}

type QueryConfig struct {
	PreferStreaming  bool   `yaml:"prefer_streaming"`
	ExecutionMode    string `yaml:"execution_mode"` // auto, streaming, pagination
}

type MCPConfig struct {  // MCP configuration
	Enabled     bool   `yaml:"enabled"`      // Whether MCP server is enabled
	Transport   string `yaml:"transport"`    // Transport protocol: stdio, http, sse
	Port        string `yaml:"port"`         // Port for HTTP transport
	Host        string `yaml:"host"`         // Host for HTTP transport
}

func Load() (*Config, error) {
	// Set defaults
	setDefaults()

	// Configure viper
	viper.SetConfigName("config")
	viper.AddConfigPath("./configs")
	viper.AddConfigPath(".")

	// Read config file
	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	var config Config
	
	// Unmarshal config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	return &config, nil
}

func setDefaults() {
	// Server defaults
	viper.SetDefault("server.port", "8080")
	viper.SetDefault("server.mode", "debug")
	viper.SetDefault("server.host", "0.0.0.0")

	// Database defaults
	viper.SetDefault("database.host", "localhost")
	viper.SetDefault("database.port", "3306")
	viper.SetDefault("database.database", "gateway_db")
	viper.SetDefault("database.username", "gateway_user")
	viper.SetDefault("database.ssl", "false")

	// Security defaults
	viper.SetDefault("security.jwt_secret", "your-secret-key")
	viper.SetDefault("security.jwt_expiration", "24h")
	viper.SetDefault("security.rate_limit_per_minute", 60)
	viper.SetDefault("security.rate_limit_burst", 10)
	viper.SetDefault("security.enable_auth", true)
	viper.SetDefault("security.enable_rate_limit", true)

	// Logging defaults
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.format", "json")

	// Query defaults
	viper.SetDefault("query.prefer_streaming", true)
	viper.SetDefault("query.execution_mode", "auto") // auto, streaming, pagination

	// MCP defaults
	viper.SetDefault("mcp.enabled", false)
	viper.SetDefault("mcp.transport", "stdio")
	viper.SetDefault("mcp.port", "8081")
	viper.SetDefault("mcp.host", "0.0.0.0")
}