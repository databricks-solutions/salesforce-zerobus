-- Onboard a Salesforce org by inserting registry rows in Lakebase. The service
-- picks up changes within SFZB_TENANTS_POLL_INTERVAL (default 30s); no deploy.
-- Secrets are references only. Store the value as a Unity Catalog secret in the
-- bundle's secrets schema first, e.g. (value in a file, not on the command line):
--   databricks secrets-uc create-secret --json @acme_prod.json
--   where acme_prod.json = {"catalog_name": "main", "schema_name": "salesforce_zerobus_secrets",
--                           "name": "acme_prod", "value": "{\"client_secret\": \"...\"}"}

INSERT INTO sfzb.tenants (tenant_key, org_id, instance_url, auth_type, client_id, client_secret_ref, labels)
VALUES ('acme-prod', '00D5g000000AbCdEAA', 'https://acme.my.salesforce.com', 'oauth_client_credentials',
        '3MVG9...', 'uc-secret://main.salesforce_zerobus_secrets.acme_prod#client_secret', '{"tier": "gold"}');

-- One subscription per org to the standard ChangeEvents channel covers every
-- object with CDC enabled; NULL columns take service defaults
-- (SFZB_DEFAULT_TABLE, LATEST replay, batch 100, ...).
INSERT INTO sfzb.subscriptions (tenant_key, topic) VALUES ('acme-prod', '/data/ChangeEvents');

-- Route one object to its own table and backfill retained events.
INSERT INTO sfzb.subscriptions (tenant_key, topic, target_table, replay_default)
VALUES ('acme-prod', '/data/OpportunityChangeEvent', 'main.sales.opportunity_cdc', 'EARLIEST');

-- Pause or offboard.
UPDATE sfzb.tenants SET enabled = false WHERE tenant_key = 'acme-prod';
-- DELETE FROM sfzb.tenants WHERE tenant_key = 'acme-prod';  -- cascades to subscriptions

-- Operate: what is failing right now?
SELECT tenant_key, topic, state, detail, restarts, last_ack_at, owner
FROM sfzb.subscription_status
WHERE state <> 'running'
ORDER BY updated_at DESC;

-- Resume positions (hex replay IDs) and throughput per subscription.
SELECT tenant_key, topic, org_id, encode(replay_id, 'hex') AS replay_id, events_acked, updated_at
FROM sfzb.checkpoints ORDER BY updated_at;
