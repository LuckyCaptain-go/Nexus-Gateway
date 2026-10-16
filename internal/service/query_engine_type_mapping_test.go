package service

import (
	"testing"

	"github.com/RhinoLink-Tech/data-fabric/internal/model"
)

func TestStreamingQueryEngineTypeMapping(t *testing.T) {
	service := &streamingService{}
	tests := []struct {
		typeName string
		want     model.StandardizedType
	}{
		{typeName: "bigint", want: model.TypeBigInt},
		{typeName: "integer", want: model.TypeInteger},
		{typeName: "timestamp with time zone", want: model.TypeTimestamp},
		{typeName: "time with time zone", want: model.TypeTime},
		{typeName: "array(varchar)", want: model.TypeArray},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			if got := service.mapQueryEngineTypeToStandard(tt.typeName); got != tt.want {
				t.Errorf("mapQueryEngineTypeToStandard(%q) = %q, want %q", tt.typeName, got, tt.want)
			}
		})
	}
}
