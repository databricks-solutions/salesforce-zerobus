// Command zerobus streams Salesforce Change Data Capture events from many
// orgs into Databricks Delta tables through Zerobus.
//
//	zerobus [run]                        run the service (default)
//	zerobus validate                     check configuration and the tenant registry
//	zerobus migrate                      create registry/checkpoint tables and migrate Delta tables
//	zerobus tenants apply -f FILE        upsert tenants from YAML into the registry
//	zerobus tenants list|enable|disable  inspect or toggle tenants
//	zerobus checkpoint get|reset T TOPIC inspect or reset a replay checkpoint
//	zerobus bootstrap lakebase           grant the service principal Lakebase access (run as project owner)
//	zerobus bootstrap uc-secrets         grant the service principal READ SECRET on the tenant secrets schema
//	zerobus ddl TABLE                    print CREATE TABLE for a target table (to pre-create it)
//	zerobus healthcheck                  exit 0 if the local /healthz is OK (for exec probes)
//	zerobus version
//
// Settings come from SFZB_* / DATABRICKS_* environment variables (or a
// ./.env file); `zerobus run -h` lists them.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/joho/godotenv"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/app"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/checkpoint"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/checkpoint/lakebase"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/config"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/obs"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tablemgr"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/ucsecrets"
)

var version = "dev"

func main() {
	_ = godotenv.Load() // optional ./.env for local runs; real env wins
	args := os.Args[1:]
	cmd := "run"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch cmd {
	case "run":
		err = runService(ctx, args)
	case "validate":
		err = validate(ctx, args)
	case "migrate":
		err = migrate(ctx, args)
	case "tenants":
		err = tenants(ctx, args)
	case "checkpoint":
		err = checkpointCmd(ctx, args)
	case "bootstrap":
		err = bootstrap(ctx, args)
	case "ddl":
		if len(args) != 1 {
			err = errors.New("usage: zerobus ddl catalog.schema.table")
			break
		}
		var ddl string
		if ddl, err = tablemgr.CreateTableDDL(args[0]); err == nil {
			fmt.Println(ddl + ";")
		}
	case "healthcheck":
		err = healthcheck(args)
	case "version":
		fmt.Println(version)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: zerobus <command> [flags]

commands:
  run                         run the service (default)
  validate                    check configuration and the tenant registry
  migrate                     create registry/checkpoint tables and migrate Delta tables
  tenants apply -f FILE       upsert tenants from YAML into the registry (-prune deletes others)
  tenants list                list registered tenants and subscription status
  tenants enable|disable KEY  toggle a tenant
  checkpoint get|reset TENANT TOPIC
  bootstrap lakebase -sp APP_ID -endpoint EP [-writer IDENTITY]...
  bootstrap uc-secrets -sp APP_ID -schema CATALOG.SCHEMA [-writer IDENTITY]...
  ddl TABLE                   print CREATE TABLE for a target table (to pre-create it)
  healthcheck                 exit 0 if the local /healthz is OK
  version

Service settings come from SFZB_* and DATABRICKS_* environment variables;
see "zerobus run -h".
`)
}

func loadConfig(args []string) (*config.Service, error) {
	return config.Load(args, os.Getenv)
}

func runService(ctx context.Context, args []string) error {
	cfg, err := loadConfig(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	logger, err := obs.NewLogger(os.Stdout, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return err
	}
	return app.Run(ctx, app.Options{Config: cfg, Logger: logger, Version: version})
}

func cliEnv(ctx context.Context, cfg *config.Service, needLakebase bool) (*app.Env, error) {
	logger, err := obs.NewLogger(os.Stderr, "warn", "text")
	if err != nil {
		return nil, err
	}
	return app.NewEnv(ctx, cfg, logger, needLakebase)
}

func validate(ctx context.Context, args []string) error {
	cfg, err := loadConfig(args)
	if err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	fmt.Printf("configuration OK (tenant source %s, checkpoint store %s, sink %s)\n", cfg.TenantSource, cfg.CheckpointStore, cfg.Sink)
	switch cfg.TenantSource {
	case "file":
		env, err := cliEnv(ctx, cfg, false)
		if err != nil {
			return err
		}
		snap, err := tenant.LoadFiles(cfg.TenantsFiles, tenant.LoadOptions{SecretSchemes: env.Secrets.Schemes(), ShardCount: cfg.ShardCount}, cfg.Defaults())
		if err != nil {
			return err
		}
		fmt.Printf("%d tenants, %d subscriptions\n", len(snap.Tenants), len(snap.Subscriptions()))
	case "lakebase":
		env, err := cliEnv(ctx, cfg, true)
		if err != nil {
			return err
		}
		defer env.Close()
		reg := env.Registry(cfg.ShardCount)
		snap, rowErrs, err := reg.Load(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("registry version %s: %d valid tenants, %d subscriptions, %d invalid tenants\n",
			snap.Version, len(snap.Tenants), len(snap.Subscriptions()), len(rowErrs))
		for _, e := range rowErrs {
			fmt.Printf("  invalid: %v\n", e.Err)
		}
		if len(rowErrs) > 0 {
			return fmt.Errorf("%d invalid tenant rows", len(rowErrs))
		}
	case "env":
		t := cfg.LegacyTenant.Tenant
		fmt.Printf("single-tenant mode: %s -> %s\n", t.Subscriptions[0].Key.Topic, t.Subscriptions[0].Table)
	}
	return nil
}

func migrate(ctx context.Context, args []string) error {
	cfg, err := loadConfig(args)
	if err != nil {
		return err
	}
	env, err := cliEnv(ctx, cfg, cfg.NeedsLakebase())
	if err != nil {
		return err
	}
	defer env.Close()
	tables := map[string]bool{}
	if cfg.DefaultTable != "" {
		tables[cfg.DefaultTable] = true
	}
	if env.Pool != nil {
		reg := env.Registry(cfg.ShardCount)
		if err := reg.Init(ctx); err != nil {
			return err
		}
		if err := reg.GrantWriters(ctx, cfg.RegistryWriters); err != nil {
			return err
		}
		if err := lakebase.New(env.Pool, cfg.LakebaseSchema).Init(ctx); err != nil {
			return err
		}
		fmt.Printf("Lakebase schema %s ready (registry, status, checkpoints)\n", cfg.LakebaseSchema)
		snap, _, err := reg.Load(ctx)
		if err != nil {
			return err
		}
		for t := range snap.Tables() {
			tables[t] = true
		}
	}
	if cfg.Legacy {
		tables[cfg.LegacyTenant.Tenant.Subscriptions[0].Table] = true
	}
	tm := env.TableManager()
	if tm == nil {
		fmt.Println("Delta table management is off (SFZB_SCHEMA_MODE=off or no workspace)")
		return nil
	}
	for t := range tables {
		if err := tm.Ensure(ctx, t); err != nil {
			return err
		}
		fmt.Printf("Delta table %s is up to date\n", t)
	}
	return nil
}

func tenants(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: zerobus tenants apply|list|enable|disable")
	}
	sub, args := args[0], args[1:]
	fs := flag.NewFlagSet("tenants "+sub, flag.ContinueOnError)
	file := fs.String("f", "", "tenants YAML file (apply)")
	prune := fs.Bool("prune", false, "delete tenants and subscriptions not in the file (apply)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadConfig(nil)
	if err != nil {
		return err
	}
	env, err := cliEnv(ctx, cfg, true)
	if err != nil {
		return err
	}
	defer env.Close()
	reg := env.Registry(cfg.ShardCount)
	if err := reg.Init(ctx); err != nil {
		return err
	}
	switch sub {
	case "apply":
		if *file == "" {
			return errors.New("-f FILE is required")
		}
		data, err := os.ReadFile(*file)
		if err != nil {
			return err
		}
		doc, err := tenant.ParseDocument(*file, data)
		if err != nil {
			return err
		}
		res, err := reg.Apply(ctx, doc, *prune)
		if err != nil {
			return err
		}
		fmt.Printf("upserted %d tenants and %d subscriptions; deleted %d tenants and %d subscriptions\n",
			res.TenantsUpserted, res.SubscriptionsUpserted, res.TenantsDeleted, res.SubscriptionsDeleted)
		return nil
	case "enable", "disable":
		if fs.NArg() != 1 {
			return fmt.Errorf("usage: zerobus tenants %s KEY", sub)
		}
		if err := reg.SetEnabled(ctx, fs.Arg(0), sub == "enable"); err != nil {
			return err
		}
		fmt.Printf("tenant %s %sd\n", fs.Arg(0), sub)
		return nil
	case "list":
		rows, err := env.Pool.Query(ctx, `SELECT t.tenant_key, t.enabled, coalesce(s.topic, ''), coalesce(st.state, ''), coalesce(st.detail, ''), coalesce(st.owner, '')
FROM `+qualified(cfg.LakebaseSchema, "tenants")+` t
LEFT JOIN `+qualified(cfg.LakebaseSchema, "subscriptions")+` s USING (tenant_key)
LEFT JOIN `+qualified(cfg.LakebaseSchema, "subscription_status")+` st ON st.tenant_key = t.tenant_key AND st.topic = s.topic
ORDER BY 1, 3`)
		if err != nil {
			return err
		}
		defer rows.Close()
		w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(w, "TENANT\tENABLED\tTOPIC\tSTATE\tOWNER\tDETAIL")
		for rows.Next() {
			var key, topic, state, detail, owner string
			var enabled bool
			if err := rows.Scan(&key, &enabled, &topic, &state, &detail, &owner); err != nil {
				return err
			}
			if len(detail) > 80 {
				detail = detail[:77] + "..."
			}
			fmt.Fprintf(w, "%s\t%v\t%s\t%s\t%s\t%s\n", key, enabled, topic, state, owner, detail)
		}
		w.Flush()
		return rows.Err()
	default:
		return fmt.Errorf("unknown tenants command %q", sub)
	}
}

func qualified(schema, table string) string { return `"` + schema + `"."` + table + `"` }

func checkpointCmd(ctx context.Context, args []string) error {
	if len(args) != 3 || (args[0] != "get" && args[0] != "reset") {
		return errors.New("usage: zerobus checkpoint get|reset TENANT TOPIC")
	}
	cfg, err := loadConfig(nil)
	if err != nil {
		return err
	}
	env, err := cliEnv(ctx, cfg, true)
	if err != nil {
		return err
	}
	defer env.Close()
	store := lakebase.New(env.Pool, cfg.LakebaseSchema)
	key := checkpoint.Key{Tenant: args[1], Topic: args[2]}
	if args[0] == "reset" {
		if err := store.Delete(ctx, key); err != nil {
			return err
		}
		fmt.Println("checkpoint deleted; the subscription resumes from its replay_default after a restart or config change")
		return nil
	}
	cps, err := store.LoadMany(ctx, []checkpoint.Key{key})
	if err != nil {
		return err
	}
	cp, ok := cps[key]
	if !ok {
		return errors.New("no checkpoint")
	}
	fmt.Printf("tenant=%s topic=%s org=%s table=%s replay_id=%x events_acked=%d owner=%s updated_at=%s\n",
		cp.Key.Tenant, cp.Key.Topic, cp.OrgID, cp.Table, cp.ReplayID, cp.EventsAcked, cp.Owner, cp.UpdatedAt.Format(time.RFC3339))
	return nil
}

// bootstrap runs as a deployer (e.g. from the bundle's postdeploy hook)
// using normal Databricks unified auth (profile or env), not the service
// principal.
func bootstrap(ctx context.Context, args []string) error {
	const usage = "usage: zerobus bootstrap lakebase -sp APP_ID -endpoint EP [-writer IDENTITY]...\n" +
		"       zerobus bootstrap uc-secrets -sp APP_ID -schema CATALOG.SCHEMA [-writer IDENTITY]..."
	if len(args) == 0 {
		return errors.New(usage)
	}
	target := args[0]
	fs := flag.NewFlagSet("bootstrap "+target, flag.ContinueOnError)
	sp := fs.String("sp", os.Getenv("DATABRICKS_CLIENT_ID"), "service principal application ID the service runs as")
	profile := fs.String("profile", os.Getenv("DATABRICKS_CONFIG_PROFILE"), "Databricks CLI profile of the deployer")
	endpoint := fs.String("endpoint", os.Getenv("SFZB_LAKEBASE_ENDPOINT"), "lakebase: projects/<p>/branches/<b>/endpoints/<e>")
	database := fs.String("database", "databricks_postgres", "lakebase: Postgres database")
	pgSchema := fs.String("pg-schema", "sfzb", "lakebase: service schema")
	secretsSchema := fs.String("schema", "", "uc-secrets: catalog.schema holding tenant UC secrets")
	var writers multiFlag
	fs.Var(&writers, "writer", "identity (user email, group, or SP app ID) that onboards tenants; repeatable")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *sp == "" {
		return errors.New("-sp is required")
	}
	w, err := databricks.NewWorkspaceClient(&databricks.Config{Profile: *profile})
	if err != nil {
		return err
	}
	logger, _ := obs.NewLogger(os.Stderr, "info", "text")

	switch target {
	case "lakebase":
		if *endpoint == "" {
			return errors.New("-endpoint is required")
		}
		me, err := w.CurrentUser.Me(ctx, iam.MeRequest{})
		if err != nil {
			return fmt.Errorf("resolving deployer identity: %w", err)
		}
		pool, err := lakebase.Connect(ctx, lakebase.Config{Endpoint: *endpoint, Database: *database, User: me.UserName, MaxConns: 1}, w.Postgres, logger)
		if err != nil {
			return err
		}
		defer pool.Close()
		if err := lakebase.Bootstrap(ctx, pool, *database, *sp, *pgSchema, writers, logger); err != nil {
			return err
		}
		fmt.Printf("Lakebase ready for service principal %s (connected as %s)\n", *sp, me.UserName)
		if len(writers) > 0 {
			fmt.Printf("Set SFZB_REGISTRY_WRITERS=%s on the service so it grants them registry access\n", strings.Join(writers, ","))
		}
	case "uc-secrets":
		if *secretsSchema == "" {
			return errors.New("-schema is required")
		}
		warning, err := ucsecrets.Bootstrap(ctx, w.Grants, *secretsSchema, *sp, writers, logger)
		if err != nil {
			return err
		}
		fmt.Printf("Unity Catalog secrets in %s readable by %s\n", *secretsSchema, *sp)
		if warning != "" {
			fmt.Println("WARNING:", warning)
		}
	default:
		return errors.New(usage)
	}
	return nil
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func healthcheck(args []string) error {
	fs := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	addr := fs.String("addr", os.Getenv("SFZB_ADMIN_ADDR"), "admin address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	host, port, err := net.SplitHostPort(*addr)
	if err != nil {
		host, port = "", "9090"
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + net.JoinHostPort(host, port) + "/healthz")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthz returned %s", resp.Status)
	}
	return nil
}
