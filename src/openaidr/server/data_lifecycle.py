"""Customer-directed erasure, replay and dedicated-database offboarding.

Only IDs, scope, timestamps and status leave the tenant database for the
external register. No prompts, findings, email addresses or free-text reasons.
"""
import hashlib
import json
from contextlib import nullcontext
from uuid import UUID

from psycopg import sql

from .deletion_register import Register
from .httpx import require

KINDS = {'findings': 'findings', 'investigations': 'session_investigation_jobs', 'cases': 'risk_cases'}
PUBLISHERS = 'findings,session_investigation_jobs,risk_cases,overview_summaries,summary_report_outbox,slack_outbox'
SCHEMA_VERSION = 158


def require_supported_schema(st):
    # A newer storage schema can introduce raw copies this cleanup cannot see.
    # Update this contract only after inventorying the corresponding migrations.
    if not st.scalar('''SELECT max(version)=$1 AND bool_or(version=$1 AND name=$2)
        FROM schema_migrations''', SCHEMA_VERSION, '0158_customer_data_lifecycle.sql'):
        raise ValueError('Lifecycle storage inventory does not cover this database schema')


def authority(st, cfg):
    register = Register(getattr(cfg, 'deletion_register_url', ''))
    with register.connect() as conn:
        remote = conn.execute('SELECT current_database() AS db,inet_server_addr()::text AS host,inet_server_port() AS port').fetchone()
    local = st.one('SELECT current_database() AS db,inet_server_addr()::text AS host,inet_server_port() AS port')
    if remote == local:
        raise ValueError('Deletion register must be outside the tenant database')
    return register


def normalize(kind, ids):
    require(isinstance(kind, str) and kind in KINDS, 'kind must be findings, investigations, or cases')
    require(isinstance(ids, list) and 1 <= len(ids) <= 100 and all(isinstance(i, str) for i in ids),
            'provide between 1 and 100 object IDs')
    try:
        return sorted({str(UUID(i)) for i in ids})
    except (ValueError, AttributeError):
        require(False, 'invalid object ID')


def digest(plan):
    return hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(',', ':')).encode()).hexdigest()



def preview(st, tenant, kind, ids, conn=None):
    ids = normalize(kind, ids)
    found = st.many(f'SELECT id FROM {KINDS[kind]} WHERE tenant_id=$1 AND id=ANY($2::uuid[])', tenant, ids, conn=conn)
    require(sorted(row['id'] for row in found) == ids, 'One or more objects were not found in this tenant', 404)
    jobs = ids if kind == 'investigations' else []
    if kind == 'findings':
        jobs = [row['investigation_job_id'] for row in st.many('''SELECT DISTINCT investigation_job_id
            FROM findings WHERE tenant_id=$1 AND id=ANY($2::uuid[]) AND investigation_job_id IS NOT NULL''', tenant, ids, conn=conn)]
    # A job result and its findings are copies of the same knowledge. Erase all
    # jobs sharing the snapshot so retries cannot recreate the selected result.
    snapshots = [r['snapshot_id'] for r in st.many('''SELECT DISTINCT snapshot_id FROM session_investigation_jobs
        WHERE tenant_id=$1 AND id=ANY($2::uuid[])''', tenant, jobs, conn=conn)]
    jobs = [r['id'] for r in st.many('''SELECT id FROM session_investigation_jobs
        WHERE tenant_id=$1 AND snapshot_id=ANY($2::uuid[])''', tenant, snapshots, conn=conn)]
    require(not st.scalar('''SELECT EXISTS(SELECT 1 FROM session_investigation_jobs WHERE tenant_id=$1
        AND id=ANY($2::uuid[]) AND state IN ('queued','running'))''', tenant, jobs, conn=conn),
        'Investigation is still running; cancel or complete it before deletion', 409)
    findings = st.many('''SELECT id,rule_id,primary_event_id FROM findings WHERE tenant_id=$1
        AND (id=ANY($2::uuid[]) OR investigation_job_id=ANY($3::uuid[]))''',
        tenant, ids if kind == 'findings' else [], jobs, conn=conn)
    cases = ids if kind == 'cases' else []
    # Case reviews can embed copies of the investigation. Include the complete
    # affected cases in the preview, including their review notes and links.
    if jobs:
        cases += [r['case_id'] for r in st.many('''SELECT DISTINCT case_id FROM risk_case_jobs
            WHERE tenant_id=$1 AND job_id=ANY($2::uuid[])''', tenant, jobs, conn=conn)]
    plan = dict(version=1, kind=kind, ids=ids, findings=sorted(r['id'] for r in findings),
                finding_sources=sorted({r['rule_id'] + ':' + r['primary_event_id'] for r in findings}),
                investigations=sorted(jobs), snapshots=sorted(snapshots), cases=sorted(set(cases)),
                invalidates=['activity caches', 'local summary reports', 'local Slack outbox'],
                external_copies='pending verification')
    return {'plan': plan, 'sha256': digest(plan)}


def _apply(st, tenant, request_id, plan, conn=None, applied_at=None):
    if plan.get('version') != 1 or plan.get('kind') not in KINDS:
        raise ValueError('Unsupported deletion record; activation refused')
    with (nullcontext(conn) if conn is not None else st.transaction()) as conn:
        st.execute("SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='60s'", conn=conn)
        st.execute('LOCK TABLE ' + PUBLISHERS + ' IN SHARE ROW EXCLUSIVE MODE', conn=conn)
        applied_at = applied_at or st.scalar('SELECT COALESCE((SELECT applied_at FROM lifecycle_applied_requests WHERE request_id=$1),clock_timestamp())', request_id, conn=conn)
        # Serialize with publishers before installing tombstones and removing
        # children. SQL triggers fence publications that arrive after commit.
        st.many('SELECT id FROM session_investigation_jobs WHERE tenant_id=$1 AND id=ANY($2::uuid[]) FOR UPDATE',
                tenant, plan['investigations'], conn=conn)
        for kind, keys in [('finding', plan['findings']), ('finding_source', plan['finding_sources']),
                           ('investigation', plan['investigations']), ('snapshot', plan['snapshots']), ('case', plan['cases'])]:
            for key in keys:
                st.execute('''INSERT INTO lifecycle_tombstones(tenant_id,kind,object_key) VALUES($1,$2,$3)
                    ON CONFLICT DO NOTHING''', tenant, kind, key, conn=conn)
        # Resolve by source as well as original IDs when replaying an older
        # snapshot: a restored worker could previously have used another ID.
        finding_ids = [r['id'] for r in st.many('''SELECT id FROM findings WHERE tenant_id=$1 AND
            (id=ANY($2::uuid[]) OR investigation_job_id=ANY($3::uuid[])
             OR rule_id || ':' || primary_event_id::text=ANY($4::text[]))''',
            tenant, plan['findings'], plan['investigations'], plan['finding_sources'], conn=conn)]
        for ident in finding_ids:
            st.execute("INSERT INTO lifecycle_tombstones(tenant_id,kind,object_key) VALUES($1,'finding',$2) ON CONFLICT DO NOTHING",
                       tenant, ident, conn=conn)
        st.execute('DELETE FROM response_actions WHERE tenant_id=$1 AND finding_id=ANY($2::uuid[])', tenant, finding_ids, conn=conn)
        st.execute('DELETE FROM findings WHERE tenant_id=$1 AND id=ANY($2::uuid[])', tenant, finding_ids, conn=conn)
        st.execute('''UPDATE feedback_delivery SET status='deleting',payload=NULL,next_attempt_at=clock_timestamp()
            WHERE tenant_id=$1 AND feedback_id=ANY($2::uuid[]) AND status<>'deleted' ''', tenant, plan['cases'], conn=conn)
        st.execute('DELETE FROM risk_cases WHERE tenant_id=$1 AND id=ANY($2::uuid[])', tenant, plan['cases'], conn=conn)
        st.execute('DELETE FROM session_investigation_jobs WHERE tenant_id=$1 AND snapshot_id=ANY($2::uuid[])',
                   tenant, plan['snapshots'], conn=conn)
        # Keep snapshot headers/finalization pointers but erase their raw copy.
        for snapshot in plan['snapshots']:
            st.execute('''INSERT INTO lifecycle_expired_snapshots(snapshot_id)
                SELECT id FROM session_snapshots WHERE tenant_id=$1 AND id=$2 ON CONFLICT DO NOTHING''', tenant, snapshot, conn=conn)
            st.execute('''DELETE FROM session_snapshot_items WHERE snapshot_id IN
                (SELECT id FROM session_snapshots WHERE tenant_id=$1 AND id=$2)''', tenant, snapshot, conn=conn)
        for table in ('risk_activity_snapshots', 'session_activity_summaries', 'overview_summaries'):
            st.execute(f'DELETE FROM {table} WHERE tenant_id=$1', tenant, conn=conn)
        for table in ('summary_report_outbox', 'slack_outbox'):
            st.execute(f'DELETE FROM {table} WHERE tenant_id=$1 AND created_at<=$2', tenant, applied_at, conn=conn)
        # Old mutation audit details may quote deleted content. Preserve minimal
        # new lifecycle evidence in the external register, not copied payloads.
        target_ids = finding_ids + plan['investigations'] + plan['cases']
        st.execute('''INSERT INTO lifecycle_applied_requests(request_id,tenant_id,applied_at) VALUES($1,$2,$3)
            ON CONFLICT(request_id) DO NOTHING''', request_id, tenant, applied_at, conn=conn)
        st.scalar('SELECT erase_lifecycle_audit($1,$2,$3::uuid[])', tenant, request_id, target_ids, conn=conn)
        return applied_at


def request(st, cfg, tenant, payload, actor):
    require(set(payload) == {'kind', 'ids', 'request_id', 'sha256'}, 'kind, ids, request_id and preview sha256 are required')
    ids = normalize(payload['kind'], payload['ids'])
    try:
        request_id = str(UUID(payload['request_id']))
    except (ValueError, TypeError, AttributeError):
        require(False, 'invalid request ID')
    register = authority(st, cfg)
    with register.locked(tenant) as (remote, _):
        existing = remote.execute('SELECT tenant_id,plan,applied_at FROM lifecycle_requests WHERE request_id=%s', (request_id,)).fetchone()
        if existing:
            plan = existing['plan']
            require(str(existing['tenant_id']) == tenant and plan['kind'] == payload['kind'] and plan['ids'] == ids
                    and digest(plan) == payload['sha256'], 'request ID has a different deletion scope', 409)
        with st.transaction() as conn:
            st.execute("SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='60s'", conn=conn)
            # Freeze publishers while binding the preview and committing intent.
            # No network/model call runs while these short database locks live.
            st.execute('LOCK TABLE ' + PUBLISHERS + ' IN SHARE ROW EXCLUSIVE MODE', conn=conn)
            if not existing:
                plan = preview(st, tenant, payload['kind'], ids, conn)['plan']
                require(digest(plan) == payload['sha256'], 'Deletion scope changed; preview again', 409)
                register.append(remote, tenant, request_id, plan, actor.get('id', ''))
            # If this fails, the committed external intent remains retryable.
            applied_at = _apply(st, tenant, request_id, plan, conn, existing['applied_at'] if existing else None)
        remote.execute('UPDATE lifecycle_requests SET applied_at=COALESCE(applied_at,%s) WHERE request_id=%s', (applied_at, request_id))
    return dict(request_id=request_id, status='primary_deleted', external_copies='pending verification')


def replay(st, cfg, *, restore=False):
    """Fail closed on missing authority, stale policy, pending erasure or expiry."""
    tenant = cfg.tenant_id
    if not tenant:
        raise ValueError('Lifecycle authority requires a bound tenant')
    register = authority(st, cfg)
    with register.locked(tenant) as (remote, row):
        require_supported_schema(st)
        policy = row['policy']
        with st.transaction() as conn:
            st.execute('''INSERT INTO tenant_telemetry_retention(tenant_id,mode,cleanup_threshold_percent,version)
                VALUES($1,$2,$3,$4) ON CONFLICT(tenant_id) DO UPDATE SET mode=EXCLUDED.mode,
                    cleanup_threshold_percent=EXCLUDED.cleanup_threshold_percent,version=EXCLUDED.version''',
                tenant, policy['mode'], policy['cleanup_threshold_percent'], policy['version'], conn=conn)
            st.execute('''UPDATE tenant_telemetry_retention SET last_deleted_day=GREATEST(last_deleted_day,$2::date)
                WHERE tenant_id=$1''', tenant, row['raw_deleted_through'], conn=conn)
        requests = remote.execute('SELECT request_id,plan,applied_at FROM lifecycle_requests WHERE tenant_id=%s ORDER BY requested_at,request_id', (tenant,)).fetchall()
        for item in requests:
            request_id = str(item['request_id'])
            if restore or not st.scalar('SELECT EXISTS(SELECT 1 FROM lifecycle_applied_requests WHERE request_id=$1 AND tenant_id=$2)', request_id, tenant):
                applied_at = _apply(st, tenant, request_id, item['plan'], applied_at=item['applied_at'])
            else:
                applied_at = st.scalar('SELECT applied_at FROM lifecycle_applied_requests WHERE request_id=$1', request_id)
            remote.execute('UPDATE lifecycle_requests SET applied_at=COALESCE(applied_at,%s) WHERE request_id=%s', (applied_at, request_id))
        if restore:
            from . import raw_retention, telemetry_retention, gateways
            previous = None
            for _ in range(10000):
                telemetry_retention.tick(st, cfg, authority=(register, remote))
                raw_retention.cleanup(st, tenant, authority=(register, remote))
                gateways.cleanup(st, cfg)
                telemetry_retention.cleanup_keys(st, cfg)
                remaining = raw_retention.remaining(st, tenant)
                if not remaining:
                    break
                if remaining == previous:
                    raise ValueError('Restore blocked: expired raw data remains (processing, lock or cleanup failure)')
                previous = remaining
            else:
                raise ValueError('Restore cleanup budget exhausted; rerun before activation')
        revision = remote.execute('SELECT revision FROM lifecycle_tenants WHERE tenant_id=%s', (tenant,)).fetchone()['revision']
        return dict(tenant_id=tenant, state='clean', revision=revision, replayed=len(requests))


def register_tenant(st, cfg):
    tenant = cfg.tenant_id
    require(bool(tenant), 'bind OPENAIDR_TENANT_ID')
    require_supported_schema(st)
    policy = st.one('SELECT mode,cleanup_threshold_percent,version FROM tenant_telemetry_retention WHERE tenant_id=$1', tenant)
    authority(st, cfg).register(tenant, policy)
    day = st.scalar('SELECT last_deleted_day FROM tenant_telemetry_retention WHERE tenant_id=$1', tenant)
    if day:
        register = authority(st, cfg)
        with register.locked(tenant) as (remote, _):
            register.raw_deleted(remote, tenant, day)
