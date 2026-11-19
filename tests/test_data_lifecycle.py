"""Real PostgreSQL tests for erasure, failure replay and raw/result separation."""
import os
from dataclasses import replace
from datetime import datetime, timedelta

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb
import pytest

from openaidr.server import data_lifecycle as life, raw_retention, telemetry_retention
from openaidr.server.config import Config
from openaidr.server.deletion_register import Register
from openaidr.server.httpx import APIError
from openaidr.server.store import new_id, now
from test_store import db as db, tenant, sensor


from lifecycle_fixtures import finding, investigation


@pytest.fixture
def configured(db):
    url = os.environ.get('OPENAIDR_TEST_REGISTER_URL')
    if not url:
        pytest.skip('OPENAIDR_TEST_REGISTER_URL must name a separate disposable database')
    schema = 'register_' + new_id().replace('-', '')
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    register = Register(make_conninfo(url, options='-c search_path=' + schema))
    register.initialize()
    tid = tenant(db)['id']
    cfg = Config(tenant_id=tid, deletion_register_url=register.url)
    life.register_tenant(db, cfg)
    try:
        yield cfg, register
    finally:
        with psycopg.connect(url, autocommit=True) as conn:
            conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))


def submission(db, cfg, kind, ids):
    preview = life.preview(db, cfg.tenant_id, kind, ids)
    payload = dict(kind=kind, ids=ids, request_id=new_id(), sha256=preview['sha256'])
    return preview, payload


def test_erasure_is_scoped_idempotent_and_replay_safe(db, configured):
    cfg, register = configured
    tid = cfg.tenant_id
    sid = sensor(db, tid)[0]['id']
    f = finding(db, tid, sid)
    other = tenant(db)['id']
    other_f = finding(db, other, sensor(db, other)[0]['id'])
    with pytest.raises(APIError):
        life.preview(db, tid, 'findings', [other_f['id']])
    _, payload = submission(db, cfg, 'findings', [f['id']])
    assert life.request(db, cfg, tid, payload, {'id': new_id()})['status'] == 'primary_deleted'
    assert life.request(db, cfg, tid, payload, {})['status'] == 'primary_deleted'
    assert db.scalar('SELECT count(*) FROM findings WHERE tenant_id=$1', tid) == 0
    assert db.scalar('SELECT count(*) FROM findings WHERE id=$1', other_f['id']) == 1
    # Simulate an old snapshot: neither local tombstone nor applied checkpoint.
    db.execute('DELETE FROM lifecycle_tombstones WHERE tenant_id=$1', tid)
    db.execute('DELETE FROM lifecycle_applied_requests WHERE tenant_id=$1', tid)
    assert db.insert_finding(dict(tenant_id=tid, sensor_id=sid, rule_id=f['rule_id'], category='test', severity='high',
        title='RESTORED', primary_event_id=f['primary_event_id'], first_event_at=now().isoformat()))
    life.replay(db, cfg, restore=True)
    assert db.scalar('SELECT count(*) FROM findings WHERE tenant_id=$1', tid) == 0
    assert register.requests(tid)[0]['applied_at'] is not None
    assert not db.insert_finding(dict(tenant_id=tid, sensor_id=sid, rule_id=f['rule_id'], category='test', severity='high',
        title='LATE RETRY', primary_event_id=f['primary_event_id'], first_event_at=now().isoformat()))


def test_intent_survives_local_failure(db, configured, monkeypatch):
    cfg, register = configured
    f = finding(db, cfg.tenant_id, sensor(db, cfg.tenant_id)[0]['id'])
    _, payload = submission(db, cfg, 'findings', [f['id']])
    original = life._apply
    def fail(*args):
        raise RuntimeError('synthetic failure after durable intent')
    monkeypatch.setattr(life, '_apply', fail)
    with pytest.raises(RuntimeError):
        life.request(db, cfg, cfg.tenant_id, payload, {})
    assert register.requests(cfg.tenant_id)[0]['applied_at'] is None
    monkeypatch.setattr(life, '_apply', original)
    life.replay(db, cfg)
    assert db.scalar('SELECT count(*) FROM findings WHERE id=$1', f['id']) == 0


def test_restore_current_policy_and_blocked_raw(db, configured):
    cfg, register = configured
    data = investigation(db, cfg.tenant_id, state='running')
    with pytest.raises(ValueError, match='expired raw data remains'):
        life.replay(db, cfg, restore=True)
    db.execute("UPDATE session_investigation_jobs SET state='completed' WHERE id=$1", data['job'])
    # Snapshot-local policy cannot lengthen the authoritative policy.
    db.execute("UPDATE tenant_telemetry_retention SET mode='90' WHERE tenant_id=$1", cfg.tenant_id)
    assert life.replay(db, cfg, restore=True)['state'] == 'clean'
    assert db.scalar('SELECT mode FROM tenant_telemetry_retention WHERE tenant_id=$1', cfg.tenant_id) == '30'
    assert raw_retention.remaining(db, cfg.tenant_id) == 0
    with register.connect() as conn:
        conn.execute("UPDATE lifecycle_tenants SET state='offboarding' WHERE tenant_id=%s", (cfg.tenant_id,))
    with pytest.raises(ValueError, match='activation refused'):
        life.replay(db, cfg, restore=True)


def test_investigation_scope_cascades(db, configured):
    cfg, _ = configured
    data = investigation(db, cfg.tenant_id, age=1)
    preview, payload = submission(db, cfg, 'investigations', [data['job']])
    assert preview['plan']['snapshots'] == [data['snapshot']]
    assert preview['plan']['cases']
    life.request(db, cfg, cfg.tenant_id, payload, {})
    for table in ('session_investigation_jobs', 'session_investigation_attempts', 'session_investigation_submissions', 'risk_cases'):
        assert db.scalar(f'SELECT count(*) FROM {table} WHERE tenant_id=$1', cfg.tenant_id) == 0


def test_policy_written_to_external_authority(db, configured):
    cfg, register = configured
    telemetry_retention.save_settings(db, cfg.tenant_id, {'mode': '7', 'version': 1}, {}, cfg)
    with register.locked(cfg.tenant_id) as (_, row):
        assert row['policy'] == dict(mode='7', version=2, cleanup_threshold_percent=None)


def test_register_cannot_share_tenant_database(db, configured):
    cfg, _ = configured
    url = os.environ['OPENAIDR_TEST_DATABASE_URL']
    with pytest.raises(ValueError, match='outside the tenant database'):
        life.authority(db, replace(cfg, deletion_register_url=url))


def test_restore_rejects_unexamined_new_storage_schema(db, configured):
    cfg, _ = configured
    db.execute("INSERT INTO schema_migrations(version,name) VALUES(9999,'9999_new_raw_store.sql')")
    with pytest.raises(ValueError, match='storage inventory'):
        life.replay(db, cfg, restore=True)


def test_scoped_audit_erasure_works_without_general_audit_delete(db, configured, monkeypatch):
    cfg, _ = configured
    f = finding(db, cfg.tenant_id, sensor(db, cfg.tenant_id)[0]['id'])
    _, payload = submission(db, cfg, 'findings', [f['id']])
    db.execute('''INSERT INTO audit_log(id,tenant_id,actor_type,actor_id,actor_name,action,target_type,target_id,details)
        VALUES($1,$2,'user','','','finding.update','finding',$3,'{"copied_content":"REMOVE"}')''', new_id(), cfg.tenant_id, f['id'])
    role = 'erase_test_' + new_id().replace('-','')
    schema = db.scalar('SELECT current_schema()')
    with db.transaction() as conn:
        conn.execute(sql.SQL('CREATE ROLE {}').format(sql.Identifier(role)))
        conn.execute(sql.SQL('GRANT USAGE ON SCHEMA {} TO {}').format(sql.Identifier(schema),sql.Identifier(role)))
        conn.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA {} TO {}').format(sql.Identifier(schema),sql.Identifier(role)))
        conn.execute(sql.SQL('REVOKE UPDATE,DELETE ON audit_log FROM {}').format(sql.Identifier(role)))
        conn.execute(sql.SQL('GRANT EXECUTE ON FUNCTION erase_lifecycle_audit(uuid,uuid,uuid[]) TO {}').format(sql.Identifier(role)))
    original = life._apply
    def restricted(st, tenant, request_id, plan, conn, applied_at=None):
        conn.execute(sql.SQL('SET LOCAL ROLE {}').format(sql.Identifier(role)))
        return original(st, tenant, request_id, plan, conn, applied_at)
    monkeypatch.setattr(life, '_apply', restricted)
    try:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            with db.transaction() as conn:
                conn.execute(sql.SQL('SET LOCAL ROLE {}').format(sql.Identifier(role)))
                conn.execute('DELETE FROM audit_log')
        life.request(db, cfg, cfg.tenant_id, payload, {})
        assert db.scalar('SELECT count(*) FROM audit_log WHERE target_id=$1', f['id']) == 0
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            db.scalar('SELECT erase_lifecycle_audit($1,$2,$3::uuid[])', cfg.tenant_id, new_id(), [new_id()])
    finally:
        with db.transaction() as conn:
            conn.execute(sql.SQL('DROP OWNED BY {}').format(sql.Identifier(role)))
            conn.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))


def test_inflight_summary_cannot_republish_deleted_sources(db, configured):
    cfg, _ = configured
    f = finding(db, cfg.tenant_id, sensor(db, cfg.tenant_id)[0]['id'])
    captured = now().isoformat()
    _, payload = submission(db, cfg, 'findings', [f['id']])
    life.request(db, cfg, cfg.tenant_id, payload, {})
    inserted = db.execute('''INSERT INTO overview_summaries(tenant_id,roles,summary,generated_at,refresh_after)
        VALUES($1,ARRAY['it_admin'],$2,clock_timestamp(),clock_timestamp())''', cfg.tenant_id,
        Jsonb({'sources':[{'captured_at':captured}], 'text':'STALE_DELETED_CONTENT'}))
    assert inserted == 0


def test_expired_cutoff_survives_later_policy_extension(db, configured):
    cfg, register = configured
    old = investigation(db, cfg.tenant_id, age=10)
    telemetry_retention.save_settings(db, cfg.tenant_id, {'mode':'7','version':1}, {}, cfg)
    raw_retention.cleanup_configured(db, cfg, cfg.tenant_id)
    telemetry_retention.save_settings(db, cfg.tenant_id, {'mode':'90','version':2}, {}, cfg)
    # An old database may have the content and no marker, even though today's
    # policy now allows 90 days. External high-water mark still removes it.
    db.execute('DELETE FROM lifecycle_expired_snapshots WHERE snapshot_id=$1', old['snapshot'])
    db.execute('ALTER TABLE session_snapshot_items DISABLE TRIGGER session_snapshot_item_guard')
    db.execute("INSERT INTO session_snapshot_items(snapshot_id,ordinal,kind,source_id,body) VALUES($1,1,'observation',$2,'{}')", old['snapshot'], new_id())
    db.execute('ALTER TABLE session_snapshot_items ENABLE TRIGGER session_snapshot_item_guard')
    assert life.replay(db, cfg, restore=True)['state'] == 'clean'
    assert db.scalar('SELECT count(*) FROM session_snapshot_items WHERE snapshot_id=$1', old['snapshot']) == 0


def test_restore_cutoff_cannot_move_backwards_while_draining_partitions(db, configured):
    from retention_fixtures import seed
    cfg, _ = configured
    device = sensor(db, cfg.tenant_id)[0]
    telemetry_retention.save_settings(db, cfg.tenant_id, {'mode':'7','version':1}, {}, cfg)
    telemetry_retention.save_settings(db, cfg.tenant_id, {'mode':'90','version':2}, {}, cfg)
    for age in (8,10,20):
        seed(db, device, age)
    assert life.replay(db, cfg, restore=True)['state'] == 'clean'
    assert db.scalar('SELECT count(*) FROM events WHERE tenant_id=$1', cfg.tenant_id) == 0


def test_http_deletion_requires_admin_csrf_and_tenant_scope(db, configured):
    from fastapi.testclient import TestClient
    from openaidr.server.app import create_app
    from openaidr.server import auth
    cfg, _ = configured
    cfg = replace(cfg, services={'api':True}, database_url=os.environ['OPENAIDR_TEST_DATABASE_URL'])
    tid = cfg.tenant_id
    admin = db.create_user(tid, 'admin@example.test', 'Admin', 'hash', 'owner')
    viewer = db.create_user(tid, 'viewer@example.test', 'Viewer', 'hash', 'viewer')
    def headers(user, csrf=True):
        return {'Cookie':auth.CONSOLE_COOKIE+'='+db.create_session(user['id'],tid),
                **({auth.CSRF_HEADER:'1'} if csrf else {})}
    f = finding(db, tid, sensor(db, tid)[0]['id'])
    client = TestClient(create_app(cfg, db, customer=True))
    path = '/api/v1/data-deletions/preview'
    body = {'kind':'findings','ids':[f['id']]}
    assert client.post(path,json=body,headers={auth.CSRF_HEADER:'1'}).status_code == 401
    assert client.post(path,json=body,headers=headers(viewer)).status_code == 403
    assert client.post(path,json=body,headers=headers(admin,False)).status_code == 403
    preview = client.post(path,json=body,headers=headers(admin))
    assert preview.status_code == 200, preview.text
    body.update(request_id=new_id(),sha256=preview.json()['sha256'])
    result = client.post('/api/v1/data-deletions',json=body,headers=headers(admin))
    assert result.status_code == 200, result.text
    assert result.json()['status'] == 'primary_deleted'


def test_worker_authority_failure_blocks_cleanup(monkeypatch):
    import asyncio
    from types import SimpleNamespace
    import starlette.concurrency
    from openaidr.server import gateways
    calls = []
    class Stop:
        done = False
        def is_set(self): return self.done
        async def wait(self): self.done = True
    async def inline(fn, *args): return fn(*args)
    def unavailable(*args): raise ValueError('authority unavailable')
    monkeypatch.setattr(starlette.concurrency, 'run_in_threadpool', inline)
    monkeypatch.setattr(life, 'replay', unavailable)
    monkeypatch.setattr(telemetry_retention, 'tick', lambda *args: calls.append('tick'))
    monkeypatch.setattr(telemetry_retention, 'cleanup_keys', lambda *args: calls.append('keys'))
    monkeypatch.setattr(gateways, 'cleanup', lambda *args: calls.append('gateway'))
    st = SimpleNamespace(many=lambda *args: calls.append('inventory'))
    asyncio.run(raw_retention.worker(st, SimpleNamespace(tenant_id='bound',deletion_register_url='synthetic'), Stop()))
    assert calls == []


def test_erasure_replay_preserves_new_notifications(db, configured):
    cfg, register = configured
    tid = cfg.tenant_id
    f = finding(db, tid, sensor(db, tid)[0]['id'])
    _, payload = submission(db, cfg, 'findings', [f['id']])
    db.execute('INSERT INTO tenant_slack_settings(tenant_id) VALUES($1)', tid)
    def queue(at):
        ident = new_id()
        db.execute("INSERT INTO slack_outbox(id,tenant_id,event_key,message,created_at) VALUES($1,$2,$1,'{}',$3)", ident, tid, at)
        db.execute("INSERT INTO summary_report_outbox(id,tenant_id,source,payload,destination_sha256,status,created_at) VALUES($1,$2,'manual','{}','digest','pending',$3)", ident, tid, at)
        return ident
    old = queue(now() - timedelta(days=1))
    life.request(db, cfg, tid, payload, {})
    applied = datetime.fromisoformat(register.requests(tid)[0]['applied_at'])
    fresh = queue(now())
    life.request(db, cfg, tid, payload, {})
    life.replay(db, cfg, restore=True)
    # Old-snapshot notifications must still be removed even if checkpoints survived.
    restored = queue(applied - timedelta(seconds=1))
    life.replay(db, cfg, restore=True)
    for table in ('slack_outbox', 'summary_report_outbox'):
        assert db.scalar(f'SELECT count(*) FROM {table} WHERE id=ANY($1::uuid[])', [old,restored]) == 0
        assert db.scalar(f'SELECT count(*) FROM {table} WHERE id=$1', fresh) == 1
    assert datetime.fromisoformat(register.requests(tid)[0]['applied_at']) == applied
    assert db.scalar('SELECT applied_at FROM lifecycle_applied_requests WHERE request_id=$1', payload['request_id']) == applied


@pytest.mark.parametrize('kind', [[], {}, None])
def test_malformed_kind_is_a_client_error(kind):
    with pytest.raises(APIError) as error:
        life.normalize(kind, [new_id()])
    assert error.value.status == 400
