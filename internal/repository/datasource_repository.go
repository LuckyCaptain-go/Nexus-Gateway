package repository

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"nexus-gateway/internal/model"
)

type dataSourceRepository struct {
	db *gorm.DB
}

// NewDataSourceRepository creates a new instance of DataSourceRepository
func NewDataSourceRepository(db *gorm.DB) DataSourceRepository {
	return &dataSourceRepository{db: db}
}

// Create a new data source
func (r *dataSourceRepository) Create(ctx context.Context, dataSource *model.DataSource) error {
	return r.db.WithContext(ctx).Create(dataSource).Error
}

// GetByID retrieves a data source by its UUID
func (r *dataSourceRepository) GetByID(ctx context.Context, id string) (*model.DataSource, error) {
	var dataSource model.DataSource
	result := r.db.WithContext(ctx).Where("id = ?", id).First(&dataSource)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrDataSourceNotFound
		}
		return nil, result.Error
	}
	return &dataSource, nil
}

// GetByName retrieves a data source by its name
func (r *dataSourceRepository) GetByName(ctx context.Context, name string) (*model.DataSource, error) {
	var dataSource model.DataSource
	result := r.db.WithContext(ctx).Where("name = ?", name).First(&dataSource)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrDataSourceNotFound
		}
		return nil, result.Error
	}
	return &dataSource, nil
}

// GetAll retrieves all data sources with optional filtering
func (r *dataSourceRepository) GetAll(ctx context.Context, status model.DataSourceStatus, limit, offset int) ([]*model.DataSource, int64, error) {
	var dataSources []*model.DataSource
	var total int64

	query := r.db.WithContext(ctx).Model(&model.DataSource{})

	// Apply status filter if provided
	if status != "" {
		query = query.Where("status = ?", status)
	}

	// Get total count
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	// Get data sources with pagination
	result := query.Limit(limit).Offset(offset).Order("created_at DESC").Find(&dataSources)
	if result.Error != nil {
		return nil, 0, result.Error
	}

	return dataSources, total, nil
}

// Update updates an existing data source
func (r *dataSourceRepository) Update(ctx context.Context, dataSource *model.DataSource) error {
	return r.db.WithContext(ctx).Save(dataSource).Error
}

// Delete soft deletes a data source
func (r *dataSourceRepository) Delete(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Where("id = ?", id).Delete(&model.DataSource{}).Error
}

// Activate sets a data source status to active
func (r *dataSourceRepository) Activate(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Model(&model.DataSource{}).Where("id = ?", id).Update("status", model.DataSourceStatusActive).Error
}

// Deactivate sets a data source status to inactive
func (r *dataSourceRepository) Deactivate(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Model(&model.DataSource{}).Where("id = ?", id).Update("status", model.DataSourceStatusInactive).Error
}

// SetError sets a data source status to error
func (r *dataSourceRepository) SetError(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Model(&model.DataSource{}).Where("id = ?", id).Update("status", model.DataSourceStatusError).Error
}

// GetActiveByType retrieves all active data sources of a specific type
func (r *dataSourceRepository) GetActiveByType(ctx context.Context, dbType model.DatabaseType) ([]*model.DataSource, error) {
	var dataSources []*model.DataSource
	result := r.db.WithContext(ctx).Where("type = ? AND status = ?", dbType, model.DataSourceStatusActive).Find(&dataSources)
	if result.Error != nil {
		return nil, result.Error
	}
	return dataSources, nil
}

// CountByStatus returns the count of data sources by status
func (r *dataSourceRepository) CountByStatus(ctx context.Context) (map[model.DataSourceStatus]int64, error) {
	var results []struct {
		Status model.DataSourceStatus
		Count  int64
	}

	err := r.db.WithContext(ctx).Model(&model.DataSource{}).Select("status, COUNT(*) as count").Group("status").Scan(&results).Error
	if err != nil {
		return nil, err
	}

	counts := make(map[model.DataSourceStatus]int64)
	for _, result := range results {
		counts[result.Status] = result.Count
	}

	return counts, nil
}
