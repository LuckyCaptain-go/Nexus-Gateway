package utils

import (
	"testing"

	"github.com/RhinoLink-Tech/data-fabric/internal/model"
)

func TestMapQueryEngineTypes(t *testing.T) {
	mapper := NewDataTypeMapper()
	tests := []struct {
		columnType string
		want       model.StandardizedType
	}{
		{columnType: "bigint", want: model.TypeBigInt},
		{columnType: "integer", want: model.TypeInteger},
		{columnType: "decimal(12, 2)", want: model.TypeDecimal},
		{columnType: "double", want: model.TypeDouble},
		{columnType: "timestamp with time zone", want: model.TypeTimestamp},
		{columnType: "time with time zone", want: model.TypeTime},
		{columnType: "array(bigint)", want: model.TypeArray},
		{columnType: "row(id bigint)", want: model.TypeStruct},
		{columnType: "map(varchar, bigint)", want: model.TypeJSON},
		{columnType: "uuid", want: model.TypeUUID},
	}

	for _, dbType := range []model.DatabaseType{
		model.DatabaseTypeTrino,
		model.DatabaseTypePresto,
		model.DatabaseTypeOpenLooKeng,
	} {
		for _, tt := range tests {
			t.Run(string(dbType)+"/"+tt.columnType, func(t *testing.T) {
				if got := mapper.MapToStandardType(dbType, tt.columnType, true); got != tt.want {
					t.Errorf("MapToStandardType(%s, %q) = %q, want %q", dbType, tt.columnType, got, tt.want)
				}
			})
		}
	}
}
