"""Bounded expiry of raw copies, preserving investigation and review results."""
from datetime import datetime, timedelta, timezone

from psycopg.types.json import Jsonb

from .store import now, new_id


def cutoff(st, tenant, conn=None):
    rows = st.many('SELECT mode,last_deleted_day FROM tenant_telemetry_retention WHERE tenant_id=$1', tenant, conn=conn)
    if not rows:
        return None
    policy = rows[0]
    limits = []
    if policy['mode'] in ('7', '30', '90'):
        # Same whole UTC storage-day boundary as native telemetry partitions.
        limits.append(datetime.combine((now() - timedelta(days=int(policy['mode']))).date(),
                                       datetime.min.time(), timezone.utc))
    if policy['last_deleted_day']:
        limits.append(datetime.combine(policy['last_deleted_day'] + timedelta(days=1),
                                       datetime.min.time(), timezone.utc))
    return max(limits) if limits else None


# Attempts retain report/result, budget and accounting metadata. These children
# contain retry payloads, copied source text, and intermediate model inputs.
ATTEMPT_RAW = ('session_investigation_submissions', 'session_investigation_reads')


def cleanup_configured(st, cfg, tenant):
    if getattr(cfg, 'deletion_register_url', ''):
        from .data_lifecycle import authority
        register = authority(st, cfg)
        with register.locked(tenant) as (remote, _):
            return cleanup(st, tenant, authority=(register, remote))
    return cleanup(st, tenant)


def cleanup(st, tenant, *, limit=100, authority=None):
    with st.transaction() as conn:
        st.execute("SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='100ms'", conn=conn)
        if not st.scalar('SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))',
                         'telemetry-retention:' + tenant, conn=conn):
            return 0
        before = cutoff(st, tenant, conn)
        if before is None:
            return 0
        if authority:
            authority[0].raw_deleted(authority[1], tenant, before.date() - timedelta(days=1))
        changed = 0
        snapshots = st.many('''SELECT s.id FROM session_snapshots s WHERE s.tenant_id=$1 AND s.created_at<$2
            AND NOT EXISTS(SELECT 1 FROM lifecycle_expired_snapshots e WHERE e.snapshot_id=s.id)
            AND NOT EXISTS(SELECT 1 FROM session_investigation_jobs j WHERE j.snapshot_id=s.id AND j.state IN ('queued','running'))
            AND NOT EXISTS(SELECT 1 FROM session_investigation_attempts a JOIN session_investigation_jobs j ON j.id=a.job_id
                WHERE j.snapshot_id=s.id AND (
                    EXISTS(SELECT 1 FROM session_investigation_requests r WHERE r.attempt_id=a.id AND r.state IN ('dispatched','uncertain'))
                    OR EXISTS(SELECT 1 FROM session_investigation_remote_messages m WHERE m.attempt_id=a.id AND m.operation_kind='finish_call' AND m.ack_revision IS NULL)))
            ORDER BY s.created_at,s.id LIMIT $3 FOR UPDATE OF s SKIP LOCKED''', tenant, before, limit, conn=conn)
        for snapshot in snapshots:
            st.execute('INSERT INTO lifecycle_expired_snapshots(snapshot_id) VALUES($1) ON CONFLICT DO NOTHING', snapshot['id'], conn=conn)
            changed += st.execute('DELETE FROM session_snapshot_items WHERE snapshot_id=$1', snapshot['id'], conn=conn)
            changed += 1
        # Lock the job before checking terminal status, shared with controllers.
        attempts = st.many('''SELECT a.id FROM session_investigation_attempts a
            JOIN session_investigation_jobs j ON j.id=a.job_id
            JOIN session_snapshots source ON source.id=j.snapshot_id
            WHERE a.tenant_id=$1 AND LEAST(a.created_at,source.created_at)<$2
              AND EXISTS(SELECT 1 FROM lifecycle_expired_snapshots e WHERE e.snapshot_id=source.id)
              AND NOT EXISTS(SELECT 1 FROM session_investigation_requests r WHERE r.attempt_id=a.id AND r.state IN ('dispatched','uncertain'))
              AND NOT EXISTS(SELECT 1 FROM session_investigation_remote_messages m WHERE m.attempt_id=a.id AND m.operation_kind='finish_call' AND m.ack_revision IS NULL)
              AND j.state NOT IN ('queued','running') AND a.state NOT IN ('reserved','dispatched')
              AND (EXISTS(SELECT 1 FROM session_investigation_steps s WHERE s.attempt_id=a.id AND (s.input_manifest<>'{}'::jsonb OR COALESCE(s.result,'{}'::jsonb)<>'{}'::jsonb))
                OR EXISTS(SELECT 1 FROM session_investigation_reads r WHERE r.attempt_id=a.id)
                OR EXISTS(SELECT 1 FROM session_investigation_submissions s WHERE s.attempt_id=a.id AND s.closed_at IS NOT NULL)
                OR EXISTS(SELECT 1 FROM session_investigation_remote_bindings b WHERE b.attempt_id=a.id
                AND (b.profile<>'{}'::jsonb OR b.create_request<>'{}'::jsonb OR b.publication IS NOT NULL))
                OR EXISTS(SELECT 1 FROM session_investigation_remote_messages m WHERE m.attempt_id=a.id AND m.operation_kind IS DISTINCT FROM 'finish_call'))
              AND NOT EXISTS(SELECT 1 FROM session_investigation_submissions s WHERE s.attempt_id=a.id AND s.closed_at IS NULL)
            ORDER BY a.created_at,a.id LIMIT $3 FOR UPDATE OF j,a SKIP LOCKED''', tenant, before, limit, conn=conn)
        ids = [a['id'] for a in attempts]
        for table in ATTEMPT_RAW:
            changed += st.execute(f'DELETE FROM {table} WHERE attempt_id=ANY($1::uuid[])', ids, conn=conn)
        # Preserve request FKs, reservations and settlement receipts; expire only payloads.
        changed += st.execute("""UPDATE session_investigation_steps SET input_manifest='{}',result=CASE WHEN result IS NULL THEN NULL ELSE '{}'::jsonb END
            WHERE attempt_id=ANY($1::uuid[]) AND (input_manifest<>'{}'::jsonb OR COALESCE(result,'{}'::jsonb)<>'{}'::jsonb)""", ids, conn=conn)
        changed += st.execute("""UPDATE session_investigation_remote_bindings SET profile='{}',create_request='{}',publication=NULL
            WHERE attempt_id=ANY($1::uuid[]) AND (profile<>'{}'::jsonb OR create_request<>'{}'::jsonb OR publication IS NOT NULL)""", ids, conn=conn)
        changed += st.execute("DELETE FROM session_investigation_remote_messages WHERE attempt_id=ANY($1::uuid[]) AND operation_kind IS DISTINCT FROM 'finish_call'", ids, conn=conn)
        changed += st.execute('''DELETE FROM session_delivery_receipts WHERE id IN (
            SELECT id FROM session_delivery_receipts WHERE tenant_id=$1 AND received_at<$2
            ORDER BY received_at,id LIMIT $3 FOR UPDATE SKIP LOCKED)''', tenant, before, limit, conn=conn)
        # Review labels/notes and evaluated decisions stay; full assessment input
        # is nullable and explicitly marked expired instead of falsifying hashes.
        changed += st.execute('''UPDATE ordinary_hook_checks SET assessment_input=NULL,
                assessment_input_sha256=NULL,assessment_context=NULL,raw_expired_at=clock_timestamp()
            WHERE id IN (SELECT id FROM ordinary_hook_checks WHERE tenant_id=$1 AND created_at<$2
                AND raw_expired_at IS NULL ORDER BY created_at,id LIMIT $3 FOR UPDATE SKIP LOCKED)''',
            tenant, before, limit, conn=conn)
        changed += st.execute('''DELETE FROM ordinary_hook_review_evidence WHERE (check_id,sha256) IN (
            SELECT e.check_id,e.sha256 FROM ordinary_hook_review_evidence e JOIN ordinary_hook_checks c ON c.id=e.check_id
            WHERE c.tenant_id=$1 AND c.raw_expired_at IS NOT NULL LIMIT $2)''', tenant, limit, conn=conn)
        # These are raw per-check inputs, not the resulting assessment.
        changed += st.execute('''UPDATE inline_guardrail_events SET target_body='{}',captured_context='{}',
                prepared_state='{}',knowledge_snapshot=NULL,raw_expired_at=clock_timestamp()
            WHERE id IN (SELECT id FROM inline_guardrail_events WHERE tenant_id=$1 AND created_at<$2
                AND status<>'pending' AND raw_expired_at IS NULL ORDER BY created_at,id LIMIT $3 FOR UPDATE SKIP LOCKED)''',
            tenant, before, limit, conn=conn)
        changed += st.execute('''UPDATE inline_guardrail_assessments SET prepared_state='{}',knowledge_snapshot=NULL
            WHERE (guardrail_event_id,generation) IN (
                SELECT a.guardrail_event_id,a.generation FROM inline_guardrail_assessments a
                JOIN inline_guardrail_events e ON e.id=a.guardrail_event_id WHERE e.tenant_id=$1
                  AND e.raw_expired_at IS NOT NULL AND a.status<>'pending'
                  AND (a.prepared_state<>'{}'::jsonb OR a.knowledge_snapshot IS NOT NULL)
                LIMIT $2 FOR UPDATE OF a SKIP LOCKED)''', tenant, limit, conn=conn)
        if changed:
            st.execute('''INSERT INTO audit_log(id,tenant_id,actor_type,actor_id,actor_name,action,target_type,target_id,details)
                VALUES($1,$2,'system','','Retention worker','raw.retention.delete','telemetry',$2,$3)''',
                new_id(), tenant, Jsonb({'before': before.isoformat(), 'rows': changed}), conn=conn)
        return changed


def remaining(st, tenant):
    """Count obligations, including blocked work: restore cannot quietly skip it."""
    before = cutoff(st, tenant)
    if before is None:
        return 0
    return st.scalar('''SELECT
        (SELECT count(*) FROM telemetry_partition_days WHERE tenant_id=$1 AND storage_day<($2 AT TIME ZONE 'UTC')::date AND retired_at IS NULL)
        + (SELECT count(*) FROM session_delivery_receipts WHERE tenant_id=$1 AND received_at<$2)
        + (SELECT count(*) FROM session_snapshots s WHERE tenant_id=$1 AND created_at<$2
            AND NOT EXISTS(SELECT 1 FROM lifecycle_expired_snapshots e WHERE e.snapshot_id=s.id))
        + (SELECT count(*) FROM ordinary_hook_checks WHERE tenant_id=$1 AND created_at<$2 AND raw_expired_at IS NULL)
        + (SELECT count(*) FROM ordinary_hook_review_evidence e JOIN ordinary_hook_checks c ON c.id=e.check_id
            WHERE c.tenant_id=$1 AND c.created_at<$2)
        + (SELECT count(*) FROM inline_guardrail_events WHERE tenant_id=$1 AND created_at<$2 AND raw_expired_at IS NULL)
        + (SELECT count(*) FROM inline_guardrail_assessments a JOIN inline_guardrail_events e ON e.id=a.guardrail_event_id
            WHERE e.tenant_id=$1 AND e.created_at<$2 AND (a.prepared_state<>'{}'::jsonb OR a.knowledge_snapshot IS NOT NULL))
        + (SELECT count(*) FROM retained_ingest_evidence WHERE tenant_id=$1 AND received_at<$2)
        + (SELECT count(*) FROM gateway_requests WHERE tenant_id=$1 AND created_at<$2)
        + (SELECT count(*) FROM session_investigation_attempts a
            JOIN session_investigation_jobs j ON j.id=a.job_id JOIN session_snapshots source ON source.id=j.snapshot_id
            WHERE a.tenant_id=$1 AND LEAST(a.created_at,source.created_at)<$2 AND (
            EXISTS(SELECT 1 FROM session_investigation_steps s WHERE s.attempt_id=a.id AND (s.input_manifest<>'{}'::jsonb OR COALESCE(s.result,'{}'::jsonb)<>'{}'::jsonb))
            OR EXISTS(SELECT 1 FROM session_investigation_reads r WHERE r.attempt_id=a.id)
            OR EXISTS(SELECT 1 FROM session_investigation_submissions s WHERE s.attempt_id=a.id)
            OR EXISTS(SELECT 1 FROM session_investigation_remote_bindings b WHERE b.attempt_id=a.id
                AND (b.profile<>'{}'::jsonb OR b.create_request<>'{}'::jsonb OR b.publication IS NOT NULL))
                OR EXISTS(SELECT 1 FROM session_investigation_remote_messages m WHERE m.attempt_id=a.id AND m.operation_kind IS DISTINCT FROM 'finish_call')))''', tenant, before)


def snapshot_erased(st, tenant, snapshot, conn=None):
    return st.scalar('''SELECT EXISTS(SELECT 1 FROM lifecycle_expired_snapshots WHERE snapshot_id=$2)
        OR EXISTS(SELECT 1 FROM lifecycle_tombstones WHERE tenant_id=$1 AND kind='snapshot' AND object_key=$2::text)''',
        tenant, snapshot, conn=conn)


async def worker(st, cfg, stop):
    """Isolate tenant failures while keeping authority failures fail-closed."""
    import asyncio
    import logging
    from starlette.concurrency import run_in_threadpool
    from .telemetry_retention import cleanup_keys, tick
    from .gateways import cleanup as cleanup_gateways

    log = logging.getLogger(__name__)
    next_check = 0
    needs_authority = bool(getattr(cfg, 'deletion_register_url', ''))
    authorized = not needs_authority
    while not stop.is_set():
        worked = False
        if asyncio.get_running_loop().time() >= next_check:
            next_check = asyncio.get_running_loop().time() + 60
            try:
                if needs_authority:
                    from .data_lifecycle import replay
                    await run_in_threadpool(replay, st, cfg)
                await run_in_threadpool(tick, st, cfg)
                authorized = True
            except Exception:
                authorized = not needs_authority
                log.warning('Retention policy maintenance deferred')
            if authorized:
                try:
                    tenants = await run_in_threadpool(st.many,
                        'SELECT id FROM tenants WHERE ($1::uuid IS NULL OR id=$1)', cfg.tenant_id or None)
                except Exception:
                    tenants = []
                    log.warning('Raw retention inventory deferred')
                for tenant in tenants:
                    try:
                        worked = bool(await run_in_threadpool(cleanup_configured, st, cfg, tenant['id'])) or worked
                    except Exception:
                        log.warning('Tenant raw retention deferred')
        if authorized:
            for cleanup in (cleanup_keys, cleanup_gateways):
                try:
                    worked = bool(await run_in_threadpool(cleanup, st, cfg)) or worked
                except Exception:
                    log.warning('Retention cleanup deferred')
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.1 if worked else 60)
        except TimeoutError:
            pass
