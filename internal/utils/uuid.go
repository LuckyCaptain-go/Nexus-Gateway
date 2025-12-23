package utils

import (
	"github.com/google/uuid"
)

// IsValidUUID checks if a string is a valid UUID
func IsValidUUID(u string) bool {
	_, err := uuid.Parse(u)
	return err == nil
}

// GenerateUUID generates a new UUID string
func GenerateUUID() string {
	return uuid.New().String()
}

// MustParseUUID parses a UUID string and panics if invalid
func MustParseUUID(u string) uuid.UUID {
	parsed, err := uuid.Parse(u)
	if err != nil {
		panic(err)
	}
	return parsed
}

// UUIDOrNil returns nil UUID if the input is empty or invalid
func UUIDOrNil(u string) uuid.UUID {
	if u == "" {
		return uuid.Nil
	}
	if parsed, err := uuid.Parse(u); err == nil {
		return parsed
	}
	return uuid.Nil
}