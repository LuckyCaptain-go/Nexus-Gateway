package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/RhinoLink-Tech/data-fabric/internal/config"
	"github.com/RhinoLink-Tech/data-fabric/internal/controller"
	"github.com/RhinoLink-Tech/data-fabric/internal/database"
	"github.com/RhinoLink-Tech/data-fabric/internal/mcp"
	"github.com/RhinoLink-Tech/data-fabric/internal/middleware"
	"github.com/RhinoLink-Tech/data-fabric/internal/model"
	"github.com/RhinoLink-Tech/data-fabric/internal/repository"
	"github.com/RhinoLink-Tech/data-fabric/internal/security"
	"github.com/RhinoLink-Tech/data-fabric/internal/service"
	"github.com/RhinoLink-Tech/data-fabric/internal/unified_service"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("Failed to load configuration:", err)
	}

	vault, err := security.LoadCredentialVaultFromEnv()
	if err != nil {
		log.Fatal("Failed to load credential encryption keys: ", err)
	}

	// Check if MCP server mode is enabled
	if cfg.MCP.Enabled {
		if cfg.MCP.Transport != "" && cfg.MCP.Transport != "stdio" {
			log.Fatalf("unsupported MCP transport %q: only stdio is currently implemented", cfg.MCP.Transport)
		}
		log.Println("Starting Data Fabric as MCP Server...")
		if err := startMCPServer(cfg, vault); err != nil {
			log.Fatal("Failed to start MCP server:", err)
		}
		return
	}

	// Normal HTTP server mode
	if err := startHTTPServer(cfg, vault); err != nil {
		log.Fatal("Failed to start HTTP server: ", err)
	}
}

func startHTTPServer(cfg *config.Config, vault *security.CredentialVault) error {
	// Fail closed on insecure authentication settings before the metadata
	// database is touched.
	if err := cfg.ValidateSecurity(); err != nil {
		return fmt.Errorf("invalid security configuration: %w", err)
	}

	// Set Gin mode
	if cfg.Server.Mode == "release" {
		gin.SetMode(gin.ReleaseMode)
	}

	// Initialize database connection
	db, err := config.InitDatabase(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	// Auto migrate database schema
	if err := db.AutoMigrate(&model.DataSource{}); err != nil {
		return fmt.Errorf("database schema migration failed: %w", err)
	}

	// Migrate old plaintext records and rotate any ciphertext before serving requests.
	datasourceRepo, err := repository.NewDataSourceRepository(db, vault)
	if err != nil {
		return fmt.Errorf("failed to initialize data-source repository: %w", err)
	}
	migrated, err := datasourceRepo.MigrateCredentials(context.Background())
	if err != nil {
		return fmt.Errorf("credential migration failed; refusing to start: %w", err)
	}
	if migrated > 0 {
		log.Printf("Encrypted or rotated credentials for %d data sources", migrated)
	}

	// Initialize infrastructure
	connPool := database.NewConnectionPool(security.NewTokenManager(vault), vault)
	healthChecker := database.NewHealthChecker(connPool)

	// Initialize security
	jwtManager := security.NewJWTManager(cfg.Security.JWTSecret, cfg.Security.JWTExpiration)
	authMiddleware := security.NewAuthMiddleware(jwtManager)

	// Initialize rate limiting
	rateLimitConfig := middleware.RateLimiterConfig{
		RPM:             cfg.Security.RateLimitPerMinute,
		Burst:           cfg.Security.RateLimitBurst,
		CleanupInterval: 5 * time.Minute,
	}
	rateLimiter := middleware.NewRateLimiter(rateLimitConfig)

	// Initialize services
	datasourceService := service.NewDataSourceService(datasourceRepo)

	// Initialize streaming service
	streamingService := service.NewStreamingService(datasourceRepo, connPool)

	// Initialize pagination service (original query service) with streaming support
	paginationService := service.NewQueryService(datasourceRepo, connPool, cfg.Query.PreferStreaming)

	// Initialize unified query service that chooses between pagination and streaming based on config
	unifiedQueryService := unifiedservice.NewUnifiedQueryService(paginationService, streamingService, cfg)
	queryService := (service.QueryService)(unifiedQueryService)

	// Initialize controllers
	datasourceController := controller.NewDataSourceController(datasourceService)
	queryController := controller.NewQueryController(queryService)
	databaseController := controller.NewDatabaseController(datasourceRepo, healthChecker)
	healthController := controller.NewHealthController(db)
	sqlTranslationController := controller.NewSQLTranslationController() // Added SQL translation controller

	// Create Gin router
	router := newRouter(routerDeps{
		cfg:                      cfg,
		authMiddleware:           authMiddleware,
		rateLimiter:              rateLimiter,
		healthController:         healthController,
		datasourceController:     datasourceController,
		queryController:          queryController,
		databaseController:       databaseController,
		sqlTranslationController: sqlTranslationController,
	})

	// Start server
	log.Printf("Starting server on port %s", cfg.Server.Port)
	log.Printf("Health check available at: http://localhost:%s/health", cfg.Server.Port)
	log.Printf("API documentation: http://localhost:%s/swagger/index.html", cfg.Server.Port)

	if err := router.Run(":" + cfg.Server.Port); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	return nil
}

func startMCPServer(cfg *config.Config, vault *security.CredentialVault) error {
	db, err := config.InitDatabase(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	if err := db.AutoMigrate(&model.DataSource{}); err != nil {
		return fmt.Errorf("database schema migration failed: %w", err)
	}
	datasourceRepo, err := repository.NewDataSourceRepository(db, vault)
	if err != nil {
		return fmt.Errorf("failed to initialize data-source repository: %w", err)
	}
	migrated, err := datasourceRepo.MigrateCredentials(context.Background())
	if err != nil {
		return fmt.Errorf("credential migration failed; refusing to start: %w", err)
	}
	if migrated > 0 {
		log.Printf("Encrypted or rotated credentials for %d data sources", migrated)
	}

	connPool := database.NewConnectionPool(security.NewTokenManager(vault), vault)
	streamingService := service.NewStreamingService(datasourceRepo, connPool)
	paginationService := service.NewQueryService(datasourceRepo, connPool, cfg.Query.PreferStreaming)
	queryService := unifiedservice.NewUnifiedQueryService(paginationService, streamingService, cfg)
	mcpServer := mcp.NewMCPServer(queryService, datasourceRepo)

	log.Printf("Starting MCP server with transport: %s", cfg.MCP.Transport)
	return mcpServer.StartStdio()
}
