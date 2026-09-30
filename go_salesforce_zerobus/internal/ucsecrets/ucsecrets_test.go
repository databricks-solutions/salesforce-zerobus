package ucsecrets

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/catalog"
)

type fakeGrants struct {
	updates []catalog.UpdatePermissions
	catalog []catalog.EffectivePrivilegeAssignment
}

func (f *fakeGrants) Update(_ context.Context, req catalog.UpdatePermissions) (*catalog.UpdatePermissionsResponse, error) {
	f.updates = append(f.updates, req)
	return &catalog.UpdatePermissionsResponse{}, nil
}

func (f *fakeGrants) GetEffective(_ context.Context, req catalog.GetEffectiveRequest) (*catalog.EffectivePermissionsList, error) {
	return &catalog.EffectivePermissionsList{PrivilegeAssignments: f.catalog}, nil
}

func TestBootstrap(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	f := &fakeGrants{catalog: []catalog.EffectivePrivilegeAssignment{{Principal: "sp", Privileges: []catalog.EffectivePrivilege{{Privilege: "USE_CATALOG"}}}}}
	warn, err := Bootstrap(context.Background(), f, "main.sfzb_secrets", "sp", []string{"ops@example.com"}, logger)
	if err != nil || warn != "" {
		t.Fatalf("warn=%q err=%v", warn, err)
	}
	u := f.updates[0]
	if u.SecurableType != "schema" || u.FullName != "main.sfzb_secrets" || len(u.Changes) != 2 {
		t.Fatalf("update = %+v", u)
	}
	if !reflect.DeepEqual(u.Changes[0].Add, ReaderPrivileges) || u.Changes[1].Principal != "ops@example.com" || !reflect.DeepEqual(u.Changes[1].Add, WriterPrivileges) {
		t.Fatalf("changes = %+v", u.Changes)
	}

	f.catalog = nil
	warn, err = Bootstrap(context.Background(), f, "main.sfzb_secrets", "sp", nil, logger)
	if err != nil || !strings.Contains(warn, "GRANT USE CATALOG ON CATALOG main TO `sp`") {
		t.Fatalf("expected USE CATALOG warning, warn=%q err=%v", warn, err)
	}
	for _, bad := range []string{"main", "a.b.c", ".x"} {
		if _, err := Bootstrap(context.Background(), f, bad, "sp", nil, logger); err == nil {
			t.Errorf("schema %q should be rejected", bad)
		}
	}
}
