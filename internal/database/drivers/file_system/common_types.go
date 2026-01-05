package file_system

// Common metadata and result types for table-format drivers (Delta, Hudi, Iceberg)
// Consolidated here to avoid duplicate type declarations across HDFS and Ozone drivers.

type DeltaMetadata struct {
	// Delta-specific
	TableLocation string
	Format        string
	Version       int64
	PartitionCols []string

	// Generic fields used by other drivers
	TableName string
	Files     int
	SizeBytes int64
}

type DeltaQueryResult struct {
	Rows    []map[string]interface{}
	Columns []string
	// Some drivers use NumRows/Count
	Count   int
	NumRows int64
	Version int64
}

type HudiMetadata struct {
	TableLocation string
	Format        string
	TableType     string
	RecordKey     string
	PartitionPath string
}

type HudiQueryResult struct {
	Rows        []map[string]interface{}
	Columns     []string
	Count       int
	NumRows     int64
	InstantTime string
}

type IcebergMetadata struct {
	TableLocation string
	Format        string
	PartitionSpec []string
	SnapshotID    int64
}

type IcebergQueryResult struct {
	Rows       []map[string]interface{}
	Columns    []string
	Count      int
	NumRows    int64
	SnapshotID int64
}
