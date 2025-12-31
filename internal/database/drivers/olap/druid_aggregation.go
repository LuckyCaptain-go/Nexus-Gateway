package olap

import (
	"context"
	"fmt"
)

// DruidAggregationHandler handles Druid aggregations
type DruidAggregationHandler struct {
	client *DruidRESTClient
}

// NewDruidAggregationHandler creates a new aggregation handler
func NewDruidAggregationHandler(client *DruidRESTClient) *DruidAggregationHandler {
	return &DruidAggregationHandler{
		client: client,
	}
}

// ExecuteGroupByAggregation executes a groupBy aggregation
func (h *DruidAggregationHandler) ExecuteGroupByAggregation(ctx context.Context, datasource string, dimensions []string, aggregations []DruidAggregation) (*DruidQueryResult, error) {
	sql := fmt.Sprintf("SELECT %s, %s FROM %s GROUP BY %s",
		formatDimensions(dimensions),
		formatAggregations(aggregations),
		datasource,
		formatDimensions(dimensions))

	return h.client.ExecuteSQLQuery(ctx, sql)
}

// ExecuteTimeseriesAggregation executes a timeseries aggregation
func (h *DruidAggregationHandler) ExecuteTimeseriesAggregation(ctx context.Context, datasource string, granularity string, aggregations []DruidAggregation, intervals []string) (*DruidQueryResult, error) {
	sql := fmt.Sprintf("SELECT TIME_FLOOR(__time, '%s') AS timestamp, %s FROM %s WHERE __time IN (%s) GROUP BY TIME_FLOOR(__time, '%s') ORDER BY timestamp",
		granularity,
		formatAggregations(aggregations),
		datasource,
		formatIntervals(intervals),
		granularity)

	return h.client.ExecuteSQLQuery(ctx, sql)
}

// ExecuteTopNQuery executes a TopN query
func (h *DruidAggregationHandler) ExecuteTopNQuery(ctx context.Context, datasource string, dimension string, aggregations []DruidAggregation, threshold int) (*DruidQueryResult, error) {
	aggSQL := formatAggregations(aggregations)
	sql := fmt.Sprintf("SELECT %s, %s FROM %s GROUP BY %s ORDER BY %s DESC LIMIT %d",
		dimension,
		aggSQL,
		datasource,
		dimension,
		aggSQL,
		threshold)

	return h.client.ExecuteSQLQuery(ctx, sql)
}

// DruidAggregation represents a Druid aggregation
type DruidAggregation struct {
	Type      string // count, sum, avg, min, max
	Name      string
	FieldName string
}

// formatAggregations formats aggregations for Druid SQL
func formatAggregations(aggregations []DruidAggregation) string {
	result := ""
	for i, agg := range aggregations {
		if i > 0 {
			result += ", "
		}

		switch agg.Type {
		case "count":
			result += fmt.Sprintf("COUNT(*) AS %s", agg.Name)
		case "sum":
			result += fmt.Sprintf("SUM(%s) AS %s", agg.FieldName, agg.Name)
		case "avg":
			result += fmt.Sprintf("AVG(%s) AS %s", agg.FieldName, agg.Name)
		case "min":
			result += fmt.Sprintf("MIN(%s) AS %s", agg.FieldName, agg.Name)
		case "max":
			result += fmt.Sprintf("MAX(%s) AS %s", agg.FieldName, agg.Name)
		default:
			result += fmt.Sprintf("%s(*) AS %s", agg.Type, agg.Name)
		}
	}
	return result
}

// ExecutePostAggregation executes a query with post-aggregations
func (h *DruidAggregationHandler) ExecutePostAggregation(ctx context.Context, datasource string, aggregations []DruidAggregation, postAggregations []DruidPostAggregation) (*DruidQueryResult, error) {
	aggSQL := formatAggregations(aggregations)
	postAggSQL := formatPostAggregations(postAggregations)

	sql := fmt.Sprintf("SELECT %s, %s FROM %s", aggSQL, postAggSQL, datasource)

	return h.client.ExecuteSQLQuery(ctx, sql)
}

// DruidPostAggregation represents a Druid post-aggregation
type DruidPostAggregation struct {
	Type     string // arithmetic, fieldAccess
	Name     string
	Fields   []string
	Formula  string // for arithmetic
	Function string
}

// formatPostAggregations formats post-aggregations
func formatPostAggregations(postAggs []DruidPostAggregation) string {
	result := ""
	for i, postAgg := range postAggs {
		if i > 0 {
			result += ", "
		}

		switch postAgg.Type {
		case "arithmetic":
			result += fmt.Sprintf("(%s) AS %s", postAgg.Formula, postAgg.Name)
		case "fieldAccess":
			if len(postAgg.Fields) > 0 {
				result += fmt.Sprintf("%s AS %s", postAgg.Fields[0], postAgg.Name)
			}
		default:
			result += fmt.Sprintf("%s AS %s", postAgg.Function, postAgg.Name)
		}
	}
	return result
}

// ExecuteCardinalityAggregation executes a cardinality aggregation (hyperUnique)
func (h *DruidAggregationHandler) ExecuteCardinalityAggregation(ctx context.Context, datasource, dimension string) (*DruidQueryResult, error) {
	sql := fmt.Sprintf("SELECT COUNT(DISTINCT %s) AS distinct_count FROM %s", dimension, datasource)
	return h.client.ExecuteSQLQuery(ctx, sql)
}

// ExecuteApproxHistogram executes an approximate histogram query
func (h *DruidAggregationHandler) ExecuteApproxHistogram(ctx context.Context, datasource, metric string, numBuckets int) (*DruidQueryResult, error) {
	sql := fmt.Sprintf("SELECT APPROX_QUANTILE(%s, ARRAY[%s]) AS histogram FROM %s",
		metric,
		generateQuantiles(numBuckets),
		datasource)
	return h.client.ExecuteSQLQuery(ctx, sql)
}

// generateQuantiles generates quantile values
func generateQuantiles(numBuckets int) string {
	quantiles := ""
	for i := 0; i < numBuckets; i++ {
		if i > 0 {
			quantiles += ", "
		}
		value := float64(i) / float64(numBuckets)
		quantiles += fmt.Sprintf("%.2f", value)
	}
	return quantiles
}
