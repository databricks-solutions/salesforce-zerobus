package tenant

import (
	"os"
	"testing"
)

// The shipped example must stay valid.
func TestExampleTenantsFile(t *testing.T) {
	data, err := os.ReadFile("../../examples/tenants.yaml")
	if err != nil {
		t.Fatal(err)
	}
	tenants, err := Parse("examples/tenants.yaml", data, LoadOptions{SecretSchemes: []string{"env", "file", "uc-secret"}, ShardCount: 8}, BuiltinDefaults())
	if err != nil {
		t.Fatal(err)
	}
	if len(tenants) != 2 || len(tenants[0].Subscriptions) != 2 {
		t.Fatalf("tenants = %+v", tenants)
	}
}
