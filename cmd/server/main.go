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

	"github.com/gin-gonic/gin"
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
		log.Fatal("Failed to migrate database:", err)
	}

	// Initialize repositories
	datasourceRepo := repository.NewDataSourceRepository(db)

	// Initialize infrastructure
	connPool := database.NewConnectionPool()
	healthChecker := database.NewHealthChecker(connPool)

	// Initialize security
	jwtManager := security.NewJWTManager(cfg.Security.JWTSecret, cfg.Security.JWTExpiration)
	authMiddleware := security.NewAuthMiddleware(jwtManager)

	// Initialize rate limiting
	rateLimitConfig := middleware.RateLimiterConfig{
		RPM:  cfg.Security.RateLimitPerMinute,
		Burst: cfg.Security.RateLimitBurst,
		CleanupInterval: 5 * time.Minute,
	}
	rateLimiter := middleware.NewRateLimiter(rateLimitConfig)

	// Initialize services
	datasourceService := service.NewDataSourceService(datasourceRepo)
	queryService := service.NewQueryService(datasourceRepo, connPool)

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

		// Query endpoints
		queries := auth.Group("/query")
		{
			queries.POST("", queryController.ExecuteQuery)
			queries.POST("/validate", queryController.ValidateQuery)
			queries.GET("/stats", queryController.GetQueryStats)
			queries.GET("/history", queryController.GetQueryHistory)
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