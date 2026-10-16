package service

import (
	"errors"
	"strings"
	"testing"

	"github.com/RhinoLink-Tech/data-fabric/internal/security"
)

func TestSafeQueryErrorMessageHidesInternalDetails(t *testing.T) {
	err := errors.New("coordinator at internal.host returned password=do-not-leak and token=also-secret")
	message := SafeQueryErrorMessage(err)
	for _, secret := range []string{"internal.host", "do-not-leak", "also-secret"} {
		if strings.Contains(message, secret) {
			t.Errorf("public error leaked %q: %s", secret, message)
		}
	}
	if message == "" || message == err.Error() {
		t.Fatalf("expected a safe actionable message, got %q", message)
	}
	if got := SafeQueryErrorMessage(security.ErrNotSelectQuery); got != "Only read-only SELECT queries are supported." {
		t.Fatalf("read-only query error = %q", got)
	}
	if got := SafeQueryErrorMessage(security.ErrSQLSyntaxError); got != "The query is invalid or uses unsupported SQL syntax." {
		t.Fatalf("syntax error = %q", got)
	}
}
