"""Real PostgreSQL tests for erasure, failure replay and raw/result separation."""
import os
from dataclasses import replace
from datetime import timedelta

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb
import pytest

from openaidr.server import raw_retention, telemetry_retention
from openaidr.server.config import Config
from openaidr.server.httpx import APIError
from openaidr.server.store import new_id, now
from test_store import db as db, tenant, sensor


from lifecycle_fixtures import finding, investigation


def test_default_only_new_tenants(db):
    tid = tenant(db)['id']
    assert db.scalar('SELECT mode FROM tenant_telemetry_retention WHERE tenant_id=$1', tid) == '30'
    db.execute('DELETE FROM tenant_telemetry_retention WHERE tenant_id=$1', tid)
    db.migrate()
    assert telemetry_retention.get_settings(db, tid, Config())['mode'] is None


def test_raw_expiry_preserves_knowledge_and_new_data(db):
    tid = tenant(db)['id']
    old = investigation(db, tid)
    fresh = investigation(db, tid, age=1)
    f = finding(db, tid, old['sensor'])
    assert raw_retention.cleanup(db, tid) > 0
    assert db.scalar('SELECT count(*) FROM session_snapshot_items WHERE snapshot_id=$1', old['snapshot']) == 0
    assert db.scalar('SELECT count(*) FROM session_investigation_submissions WHERE attempt_id=$1', old['attempt']) == 0
    assert db.scalar('SELECT result FROM session_investigation_jobs WHERE id=$1', old['job']) == {'learning': 'KEEP'}
    assert db.scalar('SELECT report FROM session_investigation_attempts WHERE id=$1', old['attempt']) == {'learning': 'KEEP'}
    assert db.scalar('SELECT count(*) FROM findings WHERE id=$1', f['id']) == 1
    assert db.scalar('SELECT count(*) FROM session_snapshot_items WHERE snapshot_id=$1', fresh['snapshot']) == 1
    assert raw_retention.cleanup(db, tid) == 0
    from openaidr.server.session_operator import read_snapshot
    with pytest.raises(APIError) as expired:
        read_snapshot(db, tid, old['snapshot'])
    assert expired.value.status == 410
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute("INSERT INTO session_snapshot_items(snapshot_id,ordinal,kind,source_id,body) VALUES($1,2,'observation',$2,'{}')", old['snapshot'], new_id())


def test_review_raw_expires_without_erasing_customer_notes(db):
    tid = tenant(db)['id']
    sid = sensor(db, tid)[0]['id']
    check = new_id()
    db.execute('''INSERT INTO ordinary_hook_checks(id,tenant_id,sensor_id,request_id,payload_sha256,product,
        hook_event,session_id,mode,settings_version,policy_version,status,evaluated_decision,returned_decision,
        assessment_input,assessment_input_sha256,assessment_context,created_at)
        VALUES($1,$2,$3,$4,'hash','codex-cli','before','session','enforce',1,1,'evaluated','deny','deny',
               '{"raw":"RAW_ONLY"}',$5,'{}',clock_timestamp()-interval '40 days')''', check, tid, sid, new_id(), 'a'*64)
    db.execute("INSERT INTO ordinary_hook_review_evidence(check_id,sha256,evidence) VALUES($1,$2,'{\"raw\":\"RAW_ONLY\"}')", check, 'a'*64)
    db.execute('''INSERT INTO ordinary_hook_review_revisions(check_id,revision,label,note,source_id,evidence_sha256)
        VALUES($1,1,'false_positive','KEEP_NOTE',$2,$3)''', check, sid, 'a'*64)
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute("UPDATE ordinary_hook_checks SET assessment_input='{}' WHERE id=$1", check)
    raw_retention.cleanup(db, tid)
    assert db.scalar('SELECT assessment_input FROM ordinary_hook_checks WHERE id=$1', check) is None
    assert db.scalar('SELECT note FROM ordinary_hook_review_revisions WHERE check_id=$1', check) == 'KEEP_NOTE'
    assert db.scalar('SELECT evidence_sha256 FROM ordinary_hook_review_revisions WHERE check_id=$1', check) is None
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute("UPDATE ordinary_hook_review_revisions SET note='tampered' WHERE check_id=$1", check)


def test_inline_assessment_history_keeps_result_after_raw_expiry(db):
    tid = tenant(db)['id']
    data = investigation(db, tid)
    event = new_id()
    db.execute('''INSERT INTO inline_guardrail_events(id,tenant_id,sensor_id,registration_id,logical_id,event_id,
        action_commitment,payload_sha256,target_body,captured_context,prepared_state,model,rubric_version,
        status,assessment,completed_at,created_at,context_pinned,knowledge_snapshot)
        VALUES($1,$2,$3,$4,'test',$5,'digest','digest','{"raw":"REMOVE"}','{"raw":"REMOVE"}',
            '{"raw":"REMOVE"}','test',1,'scored','{"result":"KEEP"}',clock_timestamp(),
            clock_timestamp()-interval '40 days',true,'{"raw":"REMOVE"}')''',
        event, tid, data['sensor'], data['registration'], new_id())
    raw_retention.cleanup(db, tid)
    assert db.scalar('SELECT target_body FROM inline_guardrail_events WHERE id=$1', event) == {}
    row = db.one('SELECT prepared_state,knowledge_snapshot,assessment FROM inline_guardrail_assessments WHERE guardrail_event_id=$1', event)
    assert row == dict(prepared_state={}, knowledge_snapshot=None, assessment={'result':'KEEP'})
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute("UPDATE inline_guardrail_assessments SET assessment='{}' WHERE guardrail_event_id=$1", event)


def test_worker_isolates_failed_tenant_and_cleanup(monkeypatch):
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
    def raw(st, cfg, tenant):
        calls.append(tenant)
        if tenant == 'blocked': raise TimeoutError('synthetic timeout')
        return 1
    def keys(*args):
        calls.append('keys')
        raise TimeoutError('synthetic key timeout')
    monkeypatch.setattr(starlette.concurrency, 'run_in_threadpool', inline)
    monkeypatch.setattr(telemetry_retention, 'tick', lambda *args: None)
    monkeypatch.setattr(raw_retention, 'cleanup_configured', raw)
    monkeypatch.setattr(telemetry_retention, 'cleanup_keys', keys)
    monkeypatch.setattr(gateways, 'cleanup', lambda *args: calls.append('gateway'))
    st = SimpleNamespace(many=lambda *args: [{'id':'blocked'},{'id':'neighbor'}])
    asyncio.run(raw_retention.worker(st, SimpleNamespace(tenant_id=''), Stop()))
    assert calls == ['blocked','neighbor','keys','gateway']


def test_raw_expiry_preserves_unsettled_calls_and_accounting(db):
    tid = tenant(db)['id']
    data = investigation(db, tid)
    attempt, step, call, message = data['attempt'], new_id(), new_id(), new_id()
    db.execute("""INSERT INTO session_investigation_steps(id,attempt_id,ordinal,kind,state,input_manifest,input_sha256,input_bytes,result,completed_at)
        VALUES($1,$2,1,'chunk','completed','{"raw":"REMOVE"}',$3,5,'{"raw":"REMOVE"}',clock_timestamp())""", step, attempt, 'a'*64)
    db.execute("""INSERT INTO session_investigation_requests(id,attempt_id,step_id,ordinal,input_sha256,request_sha256,input_tokens_counted,
        max_output_tokens,reserved_tokens,reserved_cost_usd,price_quote,usage_id,state,completed_at)
        VALUES($1,$2,$3,1,$4,$4,1,1,2,1,'{}',$5,'uncertain',clock_timestamp())""", call, attempt, step, 'a'*64, new_id())
    db.execute("""INSERT INTO session_investigation_remote_bindings(attempt_id,tenant_id,owner_tenant_id,origin,profile,create_request)
        VALUES($1,$2,$2,'https://api.example.test','{"raw":"REMOVE"}','{"raw":"REMOVE"}')""", attempt, tid)
    db.execute("""INSERT INTO session_investigation_remote_messages(id,attempt_id,step_id,kind,message,message_sha256,
        operation_id,operation_sequence,operation_sha256,operation_kind)
        VALUES($1,$2,$3,'reply','{"accounting":"KEEP"}',$4,$5,1,$4,'finish_call')""", message, attempt, step, 'a'*64, new_id())
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute("UPDATE session_investigation_steps SET input_manifest='{}',result='{}' WHERE id=$1", step)
    raw_retention.cleanup(db, tid)
    assert db.scalar('SELECT input_manifest FROM session_investigation_steps WHERE id=$1', step) == {'raw':'REMOVE'}
    assert raw_retention.remaining(db, tid) > 0
    db.execute("UPDATE session_investigation_requests SET state='completed' WHERE id=$1", call)
    raw_retention.cleanup(db, tid)
    assert db.scalar('SELECT create_request FROM session_investigation_remote_bindings WHERE attempt_id=$1', attempt) == {'raw':'REMOVE'}
    db.execute('UPDATE session_investigation_remote_messages SET ack_revision=1 WHERE id=$1', message)
    raw_retention.cleanup(db, tid)
    assert db.one('SELECT input_manifest,result FROM session_investigation_steps WHERE id=$1', step) == dict(input_manifest={},result={})
    assert db.scalar('SELECT create_request FROM session_investigation_remote_bindings WHERE attempt_id=$1', attempt) == {}
    assert db.scalar('SELECT reserved_tokens FROM session_investigation_requests WHERE id=$1', call) == 2
    assert db.scalar('SELECT message FROM session_investigation_remote_messages WHERE id=$1', message) == {'accounting':'KEEP'}
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute("UPDATE session_investigation_steps SET input_bytes=0 WHERE id=$1", step)
    assert raw_retention.remaining(db, tid) == 0
    assert raw_retention.cleanup(db, tid) == 0
