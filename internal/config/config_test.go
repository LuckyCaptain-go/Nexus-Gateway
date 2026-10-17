package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
)

// resetViper isolates each test from the global viper state that Load
// accumulates across calls (a previously read config file stays cached).
func resetViper(t *testing.T) {
	t.Helper()
	viper.Reset()
	t.Cleanup(viper.Reset)
}

func TestSecretsFromEnv(t *testing.T) {
	resetViper(t)
	t.Setenv("DATABASE_USERNAME", "env-user")
	t.Setenv("DATABASE_PASSWORD", "env-pass")
	t.Setenv("JWT_SECRET", "env-secret")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Database.Username != "env-user" {
		t.Errorf("database.username = %q, want %q", cfg.Database.Username, "env-user")
	}
	if cfg.Database.Password != "env-pass" {
		t.Errorf("database.password = %q, want %q", cfg.Database.Password, "env-pass")
	}
	if cfg.Security.JWTSecret != "env-secret" {
		t.Errorf("security.jwt_secret = %q, want %q", cfg.Security.JWTSecret, "env-secret")
	}
}

func TestSecretsFromEnvOverrideConfigFile(t *testing.T) {
	resetViper(t)

	dir := t.TempDir()
	content := "server:\n  port: \"9999\"\ndatabase:\n  password: \"file-pass\"\n"
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir to temp dir: %v", err)
	}
	t.Cleanup(func() { os.Chdir(wd) })

	t.Setenv("DATABASE_PASSWORD", "env-pass")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Server.Port != "9999" {
		t.Errorf("server.port = %q, want %q (config file was not read)", cfg.Server.Port, "9999")
	}
	if cfg.Database.Password != "env-pass" {
		t.Errorf("database.password = %q, want %q (env must win over the file)", cfg.Database.Password, "env-pass")
	}
}
