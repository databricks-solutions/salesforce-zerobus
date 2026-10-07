"""Generate typed views over salesforce_current from the Lakebase schema registry.

Runs as a job (resources/pipeline.yml), e.g. hourly. It reads sfzb.schemas
directly from Lakebase (an OAuth database credential for the job's identity,
which needs a Postgres role with SELECT on it), and only regenerates views
whose object gained a schema version since the last run (tracked in
<view schema>.salesforce_view_state). Creating or replacing a view is metadata
only: nothing is reprocessed and the pipeline keeps running.

Views:
  <tenant>__<object>     one org's object, a typed column per field (views.py)
  all_orgs__<object>     the object across orgs: fields every org shares,
                         plus the `fields` VARIANT for the rest

  python generate_views.py --lakebase-endpoint projects/<p>/branches/production/endpoints/primary \\
      --current-table main.salesforce.salesforce_current --view-catalog main --view-schema salesforce_views
"""

import argparse
import json
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import views  # noqa: E402


class Registry:
    """Read-only access to the Lakebase schema registry."""

    def __init__(self, endpoint, database, schema):
        import psycopg
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        host = w.postgres.get_endpoint(name=endpoint).status.hosts.host
        user = w.current_user.me().user_name  # a service principal's is its application ID
        token = w.postgres.generate_database_credential(endpoint=endpoint).token
        self.conn = psycopg.connect(host=host, dbname=database, user=user, password=token, sslmode="require")
        self.table = f'"{schema}"."schemas"'

    def pairs(self):
        """(org_id, entity_name) -> (schema IDs, latest tenant_key)."""
        out = {}
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT org_id, entity_name, schema_id, tenant_key FROM {self.table} "
                        "WHERE entity_name IS NOT NULL ORDER BY first_seen_at")
            for org_id, entity, schema_id, tenant_key in cur:
                ids, _ = out.get((org_id, entity), ([], None))
                out[(org_id, entity)] = (ids + [schema_id], tenant_key)
        return out

    def schemas(self, keys):
        """(org_id, entity_name) -> schema JSON, newest first, for keys."""
        out = {}
        orgs, entities = [k[0] for k in keys], [k[1] for k in keys]
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT s.org_id, s.entity_name, s.schema_json FROM {self.table} s "
                "JOIN unnest(%s::text[], %s::text[]) AS k(org_id, entity_name) USING (org_id, entity_name) "
                "ORDER BY s.first_seen_at DESC",
                (orgs, entities),
            )
            for org_id, entity, schema_json in cur:
                out.setdefault((org_id, entity), []).append(schema_json)
        return out


def _overrides_for(overrides, entity_name):
    """{"Account.SLAExpirationDate__c": "date", "Region__c": "string"} -> per-field overrides for entity_name."""
    out = {}
    for key, typ in overrides.items():
        obj, _, field = key.rpartition(".")
        if not obj or obj == entity_name:
            out[field] = typ
    return out


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--lakebase-endpoint", required=True, help="projects/<p>/branches/<b>/endpoints/<e>")
    p.add_argument("--lakebase-database", default="databricks_postgres")
    p.add_argument("--lakebase-schema", default="sfzb")
    p.add_argument("--current-table", required=True, help="salesforce_current written by the pipeline")
    p.add_argument("--view-catalog", required=True)
    p.add_argument("--view-schema", required=True)
    p.add_argument("--object-views", default="true", help="also build all_orgs__<object> views")
    p.add_argument("--type-overrides", default="{}", help='JSON, e.g. {"Account.SLAExpirationDate__c": "date"}')
    p.add_argument("--rebuild", default="false", help="regenerate every view, not only changed ones")
    p.add_argument("--dry-run", action="store_true", help="print the DDL instead of running it")
    args = p.parse_args(argv)
    overrides = json.loads(args.type_overrides or "{}")
    rebuild = args.rebuild.lower() == "true"

    spark = SparkSession.builder.getOrCreate()
    target = f"`{args.view_catalog}`.`{args.view_schema}`"
    state_table = f"{target}.salesforce_view_state"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {state_table} (org_id STRING, entity_name STRING, fingerprint STRING, view_name STRING, generated_at TIMESTAMP)"
    )

    registry = Registry(args.lakebase_endpoint, args.lakebase_database, args.lakebase_schema)
    # Metadata first (small); schema JSON only for objects that changed.
    pairs = registry.pairs()
    state = {(r.org_id, r.entity_name): r.fingerprint for r in spark.read.table(state_table).collect()}
    changed = [
        (key, ids, tenant_key) for key, (ids, tenant_key) in sorted(pairs.items())
        if rebuild or state.get(key) != views.fingerprint(ids)
    ]
    print(f"{len(pairs)} org objects in the registry, {len(changed)} new or changed")
    if not changed:
        return
    schemas = registry.schemas([key for key, _, _ in changed])

    def run(sql):
        if args.dry_run:
            print(sql + ";\n")
        else:
            spark.sql(sql)

    done, failed = [], 0
    for (org_id, entity), ids, tenant_key in changed:
        name = views.view_name(tenant_key, org_id, entity)
        sql = views.org_view_sql(
            f"{target}.`{name}`", args.current_table, org_id, entity,
            schemas.get((org_id, entity), []), _overrides_for(overrides, entity),
        )
        try:
            run(sql)
            done.append((org_id, entity, views.fingerprint(ids), name))
        except Exception as e:  # one bad schema must not block the rest
            failed += 1
            print(f"failed to create {name}: {e}", file=sys.stderr)

    if args.object_views.lower() == "true":
        # Every org's newest schema for each object that changed.
        entities = {entity for (_, entity), _, _ in changed}
        latest = registry.schemas([key for key in pairs if key[1] in entities])
        by_entity = {}
        for (_, entity), versions in sorted(latest.items()):
            by_entity.setdefault(entity, []).append(versions[0])
        for entity, latest_by_org in by_entity.items():
            name = f"all_orgs__{views.slug(entity)}"
            try:
                run(views.object_view_sql(f"{target}.`{name}`", args.current_table, entity, latest_by_org, _overrides_for(overrides, entity)))
            except Exception as e:
                failed += 1
                print(f"failed to create {name}: {e}", file=sys.stderr)

    if done and not args.dry_run:
        spark.createDataFrame(done, "org_id string, entity_name string, fingerprint string, view_name string").createOrReplaceTempView("_view_updates")
        spark.sql(
            f"""MERGE INTO {state_table} t USING _view_updates s
ON t.org_id = s.org_id AND t.entity_name = s.entity_name
WHEN MATCHED THEN UPDATE SET fingerprint = s.fingerprint, view_name = s.view_name, generated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (org_id, entity_name, fingerprint, view_name, generated_at)
  VALUES (s.org_id, s.entity_name, s.fingerprint, s.view_name, current_timestamp())"""
        )
    print(f"generated {len(done)} org views; {failed} failed")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
