package main

import (
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

	// Check if MCP server mode is enabled
	if cfg.MCP.Enabled {
		if cfg.MCP.Transport != "" && cfg.MCP.Transport != "stdio" {
			log.Fatalf("unsupported MCP transport %q: only stdio is currently implemented", cfg.MCP.Transport)
		}
		log.Println("Starting Data Fabric as MCP Server...")
		if err := startMCPServer(cfg); err != nil {
			log.Fatal("Failed to start MCP server:", err)
		}
		return
	}

	// Normal HTTP server mode
	startHTTPServer(cfg)
}

func startHTTPServer(cfg *config.Config) {
	// Set Gin mode
	if cfg.Server.Mode == "release" {
		gin.SetMode(gin.ReleaseMode)
	}

	// Initialize database connection
	db, err := config.InitDatabase(cfg)
	if err != nil {
		log.Fatal("Failed to initialize database:", err)
	}

	// Auto migrate database schema
	if err := db.AutoMigrate(&model.DataSource{}); err != nil {
		log.Printf("Warning: Database migration failed: %v", err)
		log.Println("Continuing with existing database schema...")
	}

	// Initialize repositories
	datasourceRepo := repository.NewDataSourceRepository(db)

	// Initialize security
	// Use a 32-byte key for AES-256 encryption
	masterKey := []byte("12345678901234567890123456789012") // 32 bytes - TODO: make this configurable
	vault, err := security.NewCredentialVault(masterKey)
	if err != nil {
		log.Fatalf("Failed to create credential vault: %v", err)
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
	router := gin.New()

	// Add middleware
	router.Use(gin.Logger())
	router.Use(gin.Recovery())
	router.Use(middleware.Cors())
	router.Use(middleware.CorrelationID())

	// Authenticate optional JWTs before rate limiting so verified users share
	// a per-user bucket. Invalid or absent credentials fall back to the peer IP.
	if cfg.Security.EnableAuth && cfg.Security.EnableRateLimit {
		router.Use(authMiddleware.OptionalAuth())
	}

	// Add rate limiting if enabled
	if cfg.Security.EnableRateLimit {
		router.Use(rateLimiter.RateLimit())
	}

	// Health check endpoint (always available)
	router.GET("/health", healthController.HealthCheck)

	// API v1 group
	api := router.Group("/api/v1")

	// Public endpoints (no authentication required)
	public := api.Group("")
	{
		public.GET("/health", healthController.HealthCheck)
		public.GET("/database/types", databaseController.GetDatabaseTypes)
		public.GET("/database/compatibility", databaseController.GetDatabaseCompatibility)
	}

	// SQL translation endpoints (no authentication required)
	sqlGroup := api.Group("/sql")
	{
		sqlGroup.POST("/translate", sqlTranslationController.TranslateSQL)
		sqlGroup.GET("/dialects", sqlTranslationController.GetSupportedDialects)
		sqlGroup.POST("/validate", sqlTranslationController.ValidateSQL)
	}

	// Auth endpoints (authentication required)
	auth := api.Group("")
	if cfg.Security.EnableAuth {
		auth.Use(authMiddleware.RequireAuth())
	}
	{
		// Data source endpoints
		datasources := auth.Group("/datasources")
		{
			datasources.POST("", datasourceController.CreateDataSource)
			datasources.GET("", datasourceController.ListDataSources)
			datasources.GET("/stats", datasourceController.GetDataSourceStats)
			datasources.GET("/:id", datasourceController.GetDataSource)
			datasources.PUT("/:id", datasourceController.UpdateDataSource)
			datasources.DELETE("/:id", datasourceController.DeleteDataSource)
			datasources.POST("/:id/activate", datasourceController.ActivateDataSource)
			datasources.POST("/:id/deactivate", datasourceController.DeactivateDataSource)
		}

		// Fetch endpoints (batch data retrieval)
		fetch := api.Group("/fetch")
		{
			fetch.POST("", queryController.FetchQuery)
			fetch.GET("/:query_id/:slug/:token", queryController.FetchNextBatch)
		}

		// Database management endpoints
		database := auth.Group("/database")
		{
			database.POST("/test-connection", databaseController.TestDataSourceConnection)
			database.POST("/validate-config", databaseController.ValidateDataSourceConfig)
			database.GET("/connections/stats", databaseController.GetConnectionStats)
			database.GET("/health", databaseController.GetDatabaseHealth)
		}
	}

	// Start server
	log.Printf("Starting server on port %s", cfg.Server.Port)
	log.Printf("Health check available at: http://localhost:%s/health", cfg.Server.Port)
	log.Printf("API documentation: http://localhost:%s/swagger/index.html", cfg.Server.Port)

	if err := router.Run(":" + cfg.Server.Port); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}

func startMCPServer(cfg *config.Config) error {
	db, err := config.InitDatabase(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	if err := db.AutoMigrate(&model.DataSource{}); err != nil {
		log.Printf("Warning: Database migration failed: %v", err)
		log.Println("Continuing with existing database schema...")
	}

	datasourceRepo := repository.NewDataSourceRepository(db)
	masterKey := []byte("12345678901234567890123456789012")
	vault, err := security.NewCredentialVault(masterKey)
	if err != nil {
		return fmt.Errorf("failed to create credential vault: %w", err)
	}

	connPool := database.NewConnectionPool(security.NewTokenManager(vault), vault)
	streamingService := service.NewStreamingService(datasourceRepo, connPool)
	paginationService := service.NewQueryService(datasourceRepo, connPool, cfg.Query.PreferStreaming)
	queryService := unifiedservice.NewUnifiedQueryService(paginationService, streamingService, cfg)
	mcpServer := mcp.NewMCPServer(queryService, datasourceRepo)

	log.Printf("Starting MCP server with transport: %s", cfg.MCP.Transport)
	return mcpServer.StartStdio()
}
