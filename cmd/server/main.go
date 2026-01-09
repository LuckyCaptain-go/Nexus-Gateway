package main

import (
	"log"
	"time"

	"nexus-gateway/internal/config"
	"nexus-gateway/internal/controller"
	"nexus-gateway/internal/database"
	"nexus-gateway/internal/middleware"
	"nexus-gateway/internal/model"
	"nexus-gateway/internal/repository"
	"nexus-gateway/internal/security"
	"nexus-gateway/internal/service"
	"nexus-gateway/internal/unified_service"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("Failed to load configuration:", err)
	}

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

	// Create Gin router
	router := gin.New()

	// Add middleware
	router.Use(gin.Logger())
	router.Use(gin.Recovery())
	router.Use(middleware.Cors())
	router.Use(middleware.CorrelationID())

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

	// Internal API v2 group (for Vega-Gateway compatibility)
	internalAPI := router.Group("/api/internal/vega-gateway/v2")

	// Internal fetch endpoints
	internalFetch := internalAPI.Group("/fetch")
	{
		// Internal fetch query endpoint (requires account headers)
		internalFetch.POST("", queryController.InternalFetchQuery)

		// Internal fetch next batch endpoint (requires account headers)
		internalFetch.GET("/:query_id/:slug/:token", queryController.InternalFetchNextBatch)
	}

	// Start server
	log.Printf("Starting server on port %s", cfg.Server.Port)
	log.Printf("Health check available at: http://localhost:%s/health", cfg.Server.Port)
	log.Printf("API documentation: http://localhost:%s/swagger/index.html", cfg.Server.Port)

	if err := router.Run(":" + cfg.Server.Port); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}
