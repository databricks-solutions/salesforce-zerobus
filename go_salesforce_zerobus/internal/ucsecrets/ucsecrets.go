// Package ucsecrets prepares a Unity Catalog schema to hold tenant
// credentials as UC secrets: the service principal may read them and
// designated operators may create and rotate them.
package ucsecrets

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/databricks/databricks-sdk-go/service/catalog"
)

// GrantsAPI is the subset of the Unity Catalog grants API used here.
type GrantsAPI interface {
	Update(ctx context.Context, req catalog.UpdatePermissions) (*catalog.UpdatePermissionsResponse, error)
	GetEffective(ctx context.Context, req catalog.GetEffectiveRequest) (*catalog.EffectivePermissionsList, error)
}

// Privileges granted on the secrets schema.
var (
	ReaderPrivileges = []catalog.Privilege{"USE_SCHEMA", "READ_SECRET"}
	WriterPrivileges = []catalog.Privilege{"USE_SCHEMA", "CREATE_SECRET", "WRITE_SECRET"}
)

// Bootstrap grants sp read access to secrets in schema (catalog.schema) and
// writers the privileges to create and rotate them. Grants are additive and
// idempotent. It returns a warning (not an error) if sp cannot use the
// catalog, which a catalog owner must grant.
func Bootstrap(ctx context.Context, api GrantsAPI, schema, sp string, writers []string, logger *slog.Logger) (warning string, err error) {
	catalogName, _, ok := strings.Cut(schema, ".")
	if !ok || catalogName == "" || strings.Count(schema, ".") != 1 {
		return "", fmt.Errorf("secrets schema %q must be catalog.schema", schema)
	}
	if sp == "" {
		return "", fmt.Errorf("service principal application ID is required")
	}
	changes := []catalog.PermissionsChange{{Principal: sp, Add: ReaderPrivileges}}
	for _, w := range writers {
		changes = append(changes, catalog.PermissionsChange{Principal: w, Add: WriterPrivileges})
	}
	if _, err := api.Update(ctx, catalog.UpdatePermissions{SecurableType: "schema", FullName: schema, Changes: changes}); err != nil {
		return "", fmt.Errorf("granting secret access on %s: %w", schema, err)
	}
	logger.Info("Granted Unity Catalog secret access", "schema", schema, "reader", sp, "writers", writers)

	eff, err := api.GetEffective(ctx, catalog.GetEffectiveRequest{SecurableType: "catalog", FullName: catalogName, Principal: sp})
	if err != nil {
		return fmt.Sprintf("could not verify USE CATALOG on %s for %s: %v", catalogName, sp, err), nil
	}
	for _, a := range eff.PrivilegeAssignments {
		for _, p := range a.Privileges {
			if p.Privilege == "USE_CATALOG" || p.Privilege == "ALL_PRIVILEGES" {
				return "", nil
			}
		}
	}
	return fmt.Sprintf("%s has no USE CATALOG on %s; a catalog owner must run: GRANT USE CATALOG ON CATALOG %s TO `%s`",
		sp, catalogName, catalogName, sp), nil
}
