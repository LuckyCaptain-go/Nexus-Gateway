package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Database DatabaseConfig `mapstructure:"database"`
	Security SecurityConfig `mapstructure:"security"`
	Logging  LoggingConfig  `mapstructure:"logging"`
}

type ServerConfig struct {
	Port string `mapstructure:"port"`
	Mode string `mapstructure:"mode"`
	Host string `mapstructure:"host"`
}

type DatabaseConfig struct {
	Host     string `mapstructure:"host"`
	Port     string `mapstructure:"port"`
	Database string `mapstructure:"database"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	SSL      string `mapstructure:"ssl"`
}

type SecurityConfig struct {
	JWTSecret         string        `mapstructure:"jwt_secret"`
	JWTExpiration     time.Duration `mapstructure:"jwt_expiration"`
	RateLimitPerMinute int          `mapstructure:"rate_limit_per_minute"`
	RateLimitBurst    int           `mapstructure:"rate_limit_burst"`
	EnableAuth        bool          `mapstructure:"enable_auth"`
	EnableRateLimit   bool          `mapstructure:"enable_rate_limit"`
}

type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

func Load() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./configs")
	viper.AddConfigPath(".")

	// Set default values
	setDefaults()

	// Enable environment variable support
	viper.AutomaticEnv()

	// Read config file
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found, use defaults and environment
			fmt.Println("Config file not found, using defaults and environment variables")
		} else {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	var config Config
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
}