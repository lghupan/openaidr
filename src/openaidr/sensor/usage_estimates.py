"""Display-only estimates for retained calls that have no recorded cost."""

from pathlib import Path

from openaidr.core.agent_usage import text
from .model_pricing import TIERS, Catalog, token_cost


def initialize(db):
    # Older sensors still write and compact the original ledger schema.
    db.executescript("""
        CREATE TABLE IF NOT EXISTS usage_price_inputs (
            event_key TEXT PRIMARY KEY, happened_at REAL NOT NULL, model TEXT NOT NULL,
            effort TEXT NOT NULL, application TEXT NOT NULL, cached INTEGER NOT NULL,
            incoming INTEGER NOT NULL, outgoing INTEGER NOT NULL, written INTEGER NOT NULL,
            tier TEXT NOT NULL, retention TEXT NOT NULL, hour_writes INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS usage_price_time ON usage_price_inputs(happened_at);
        CREATE INDEX IF NOT EXISTS usage_missing_cost ON usage_calls(happened_at) WHERE cost_usd IS NULL;
        CREATE TRIGGER IF NOT EXISTS usage_price_revision AFTER INSERT ON usage_price_inputs
        BEGIN
            INSERT INTO usage_revisions SELECT scope,CAST(new.happened_at/3600 AS INTEGER),1
                FROM usage_calls WHERE event_key=new.event_key ORDER BY rowid LIMIT 1
                ON CONFLICT(scope,bucket) DO UPDATE SET revision=revision+1;
        END;
        CREATE TRIGGER IF NOT EXISTS usage_price_known AFTER UPDATE ON usage_calls
        WHEN new.cost_usd IS NOT NULL OR (new.happened_at,new.model,new.effort,new.tokens)
            IS NOT (old.happened_at,old.model,old.effort,old.tokens)
        BEGIN DELETE FROM usage_price_inputs WHERE event_key=new.event_key; END;
        CREATE TRIGGER IF NOT EXISTS usage_price_tokens_changed AFTER UPDATE ON usage_tokens
        WHEN (new.cached_input_tokens,new.uncached_input_tokens,new.output_tokens)
            IS NOT (old.cached_input_tokens,old.uncached_input_tokens,old.output_tokens)
        BEGIN DELETE FROM usage_price_inputs WHERE event_key=new.event_key; END;
        CREATE TRIGGER IF NOT EXISTS usage_price_forget AFTER DELETE ON usage_calls
        WHEN NOT EXISTS (SELECT 1 FROM usage_compacted WHERE event_key=old.event_key)
        BEGIN DELETE FROM usage_price_inputs WHERE event_key=old.event_key; END;
        CREATE TRIGGER IF NOT EXISTS usage_price_forget_compacted AFTER DELETE ON usage_compacted
        BEGIN DELETE FROM usage_price_inputs WHERE event_key=old.event_key; END;
    """)


def remember(db, events):
    rows = []
    for event in events:
        counts = event["usage"]
        if counts["cost"] is not None or not counts["tokens"]:
            continue
        attrs = event.get("attributes", {})
        written = counts["cache_write_tokens"]
        tier, retention = text(attrs, "service_tier"), text(attrs, "prompt_cache_retention")
        tier = tier if tier in TIERS else "unsupported"
        retention = retention if retention in ("", "30m", "12h") else "unsupported"
        if written or tier:
            hour_writes = attrs.get("hour_writes", 0) if event["name"] == "claude_code.api_request" else 0
            if type(hour_writes) is not int or not 0 <= hour_writes <= written:
                hour_writes = -1
            rows.append((written, tier, retention, hour_writes, event["key"], event["local_scope"],
                         event["time"].timestamp(), event["model"], event["reasoning_effort"],
                         event["name"].split(".")[0], counts["cache_read_tokens"],
                         counts["input_tokens"] + written, counts["output_tokens"]))
    db.executemany("""
        INSERT OR IGNORE INTO usage_price_inputs
        SELECT u.event_key,u.happened_at,u.model,u.effort,a.application,t.cached_input_tokens,
            t.uncached_input_tokens,t.output_tokens,?,?,?,?
        FROM usage_calls u JOIN usage_apps a USING(scope,event_key) JOIN usage_tokens t USING(scope,event_key)
        WHERE u.event_key=? AND u.scope=? AND u.happened_at=? AND u.model=? AND u.effort=? AND u.cost_usd IS NULL
            AND a.application=? AND t.cached_input_tokens=? AND t.uncached_input_tokens=? AND t.output_tokens=?
    """, rows)


def compact(db, original):
    # Keep the individual input length. Summing two short calls before applying
    # a long-context rate would change the estimate when the ledger compacts.
    db.execute(f"""
        INSERT OR IGNORE INTO usage_price_inputs
        SELECT u.event_key,u.happened_at,u.model,u.effort,coalesce(a.application,'unknown'),
            t.cached_input_tokens,t.uncached_input_tokens,t.output_tokens,0,'','',0
        FROM usage_calls u JOIN usage_tokens t USING(scope,event_key) LEFT JOIN usage_apps a USING(scope,event_key)
        WHERE {original} AND u.cost_usd IS NULL AND u.tokens>0
            AND u.tokens=t.cached_input_tokens+t.uncached_input_tokens+t.output_tokens
            AND NOT EXISTS (SELECT 1 FROM usage_recovered r WHERE r.event_key=u.event_key)
    """)


def cache_key(config):
    try:
        stat = (Path(config["data_dir"]) / "model-pricing.json").stat()
        return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns
    except OSError:
        return None


def configure(db, config, tables):
    """Create a connection-local view. The shared usage database stays read-only."""
    catalog = Catalog(config)
    rates = dict(catalog.other_models)
    rates.update((model, row[1]) for model, row in catalog.rows.items())
    db.execute("DROP VIEW IF EXISTS temp.display_price_inputs")
    if not rates or "usage_tokens" not in tables:
        return False

    def estimate(model, tier, cached, incoming, outgoing, written, retention, hour_writes):
        prices = rates.get(model)
        if (prices is None or not any((cached, incoming, outgoing)) or min(cached, incoming, outgoing, written) < 0
                or written > incoming or not 0 <= hour_writes <= written or (hour_writes and retention)):
            return None
        cost = token_cost(prices, tier, (incoming - written, cached, written, outgoing),
                          {"prompt_cache_retention": retention or None}, hour_writes=hour_writes)
        return None if cost is None or cost is NotImplemented else float(cost)

    db.create_function("display_price", 8, estimate, deterministic=True)
    has_inputs = "usage_price_inputs" in tables
    has_receipts = "usage_recovered" in tables
    has_compacted = "usage_compacted" in tables
    app = "coalesce(a.application,'unknown')" if "usage_apps" in tables else "'unknown'"
    joins = []
    if "usage_apps" in tables:
        joins.append("LEFT JOIN usage_apps a USING(scope,event_key)")
    if has_inputs:
        joins.append("LEFT JOIN usage_price_inputs p ON p.event_key=u.event_key")
    if has_receipts:
        joins.append("LEFT JOIN usage_recovered r ON r.event_key=u.event_key")
    tier = "r.service_tier" if has_receipts else "''"
    if has_inputs:
        tier = f"coalesce(nullif(p.tier,''),{tier})"
    tier = f"coalesce({tier},'')"
    written = "coalesce(p.written,0)" if has_inputs else "0"
    retention = "coalesce(p.retention,'')" if has_inputs else "''"
    hour_writes = "coalesce(p.hour_writes,0)" if has_inputs else "0"
    parts = [f"""SELECT u.event_key,u.happened_at,u.model,u.effort,{app} AS application,
        t.cached_input_tokens AS cached,t.uncached_input_tokens AS incoming,t.output_tokens AS outgoing,
        {written} AS written,{tier} AS tier,{retention} AS retention,{hour_writes} AS hour_writes,0 AS compacted
        FROM usage_calls u JOIN usage_tokens t USING(scope,event_key) {' '.join(joins)}
        WHERE u.cost_usd IS NULL AND u.tokens=t.cached_input_tokens+t.uncached_input_tokens+t.output_tokens
        AND u.rowid=(SELECT min(v.rowid) FROM usage_calls v WHERE v.event_key=u.event_key)"""]
    if has_compacted:
        parts[0] += " AND NOT EXISTS (SELECT 1 FROM usage_compacted c WHERE c.event_key=u.event_key)"
        if has_inputs:
            receipt_join = "LEFT JOIN usage_recovered r USING(event_key)" if has_receipts else ""
            parts.append(f"""SELECT p.event_key,p.happened_at,p.model,p.effort,p.application,
                p.cached,p.incoming,p.outgoing,p.written,{tier},p.retention,p.hour_writes,1
                FROM usage_price_inputs p JOIN usage_compacted c USING(event_key) {receipt_join}""")
        if has_receipts:
            part = """SELECT r.event_key,r.happened_at,r.model,r.effort,r.application,r.cached_input_tokens,
                r.uncached_input_tokens,r.output_tokens,0,r.service_tier,'',0,1 FROM usage_recovered r
                JOIN usage_compacted c USING(event_key) WHERE r.cost_usd IS NULL
                AND r.tokens=r.cached_input_tokens+r.uncached_input_tokens+r.output_tokens"""
            if has_inputs:
                part += " AND NOT EXISTS (SELECT 1 FROM usage_price_inputs p WHERE p.event_key=r.event_key)"
            parts.append(part)
    db.execute("CREATE TEMP VIEW display_price_inputs AS " + " UNION ALL ".join(parts))
    return True


def groups(db, since, until, bucket, params, slot, hourly):
    rows = db.execute(f"""
        WITH quotes AS MATERIALIZED (
            SELECT {bucket} AS bucket,model,effort,application,compacted{slot},
                display_price(model,tier,cached,incoming,outgoing,written,retention,hour_writes) AS cost
            FROM display_price_inputs u WHERE happened_at>=? AND happened_at<? {'AND compacted=0' if hourly else ''}
        )
        SELECT bucket,model,effort,application,compacted,{('hour_bucket,' if slot else '')}
            count(cost) AS estimated_calls,sum(cost) AS cost_usd
        FROM quotes WHERE cost IS NOT NULL GROUP BY bucket,model,effort,application,compacted{',hour_bucket' if slot else ''}
    """, (*params, since, until))
    return [dict(row, calls=0, priced_calls=row["estimated_calls"], tokens=0,
                 cached_input_tokens=0, uncached_input_tokens=0, output_tokens=0, unclassified_tokens=0) for row in rows]
