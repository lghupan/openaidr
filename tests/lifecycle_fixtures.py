from datetime import timedelta
from psycopg.types.json import Jsonb
from openaidr.server.store import new_id, now
from test_store import sensor


def finding(db, tid, sid, *, title='Retained learning'):
    event = new_id()
    assert db.insert_finding(dict(tenant_id=tid, sensor_id=sid, rule_id='test-rule', category='test',
        severity='high', title=title, primary_event_id=event, first_event_at=now().isoformat()))
    return db.one('SELECT * FROM findings WHERE tenant_id=$1 AND primary_event_id=$2', tid, event)


def investigation(db, tid, *, age=40, state='completed'):
    sid = sensor(db, tid)[0]['id']
    reg, snap, job, attempt = (new_id() for _ in range(4))
    old = now() - timedelta(days=age)
    db.execute('''INSERT INTO session_registrations(id,tenant_id,sensor_id,product,expires_at)
        VALUES($1,$2,$3,'codex-cli',clock_timestamp())''', reg, tid, sid)
    db.execute('''INSERT INTO session_snapshots(id,tenant_id,sensor_id,registration_id,revision,input_version,lifecycle,coverage,counts,created_at)
        VALUES($1,$2,$3,$4,1,1,'closed','{}','{}',$5)''', snap, tid, sid, reg, old)
    db.execute("INSERT INTO session_snapshot_items(snapshot_id,ordinal,kind,source_id,body) VALUES($1,1,'observation',$2,$3)",
               snap, new_id(), Jsonb({'kind': 'tool', 'raw_secret': 'RAW_ONLY'}))
    db.execute('UPDATE session_snapshots SET sealed_at=clock_timestamp(),item_count=1 WHERE id=$1', snap)
    db.execute('''INSERT INTO session_investigation_jobs(id,tenant_id,sensor_id,registration_id,snapshot_id,
        detector_config_version,settings_version,configuration,routing_reason,state,result,created_at,completed_at)
        VALUES($1,$2,$3,$4,$5,1,1,'{}','persistent_gap',$6,'{"learning":"KEEP"}',$7,$7)''', job, tid, sid, reg, snap, state, old)
    db.execute('''INSERT INTO session_investigation_attempts(id,job_id,tenant_id,ordinal,claim_token,worker_id,state,
        reserved_tokens,reserved_cost_usd,window_start,deadline_at,result,created_at,completed_at,report)
        VALUES($1,$2,$3,1,$4,'test','completed',1,1,$5,$5,'{"learning":"KEEP"}',$5,$5,'{"learning":"KEEP"}')''',
        attempt, job, tid, new_id(), old)
    db.execute('''INSERT INTO session_investigation_submissions(attempt_id,tenant_id,origin,api_tenant_id,request,request_sha256,closed_at,created_at)
        VALUES($1,$2,'https://api.example.test',$2,$3,$4,$5,$5)''', attempt, tid,
        '{"request_id":"' + attempt + '","raw":"RAW_ONLY"}', 'a'*64, old)
    db.execute('''INSERT INTO session_delivery_receipts(id,tenant_id,sensor_id,registration_id,epoch_id,record_id,kind,payload_sha256,payload,received_at)
        VALUES($1,$2,$3,$4,$5,$6,'close','digest','{"raw":"RAW_ONLY"}',$7)''', new_id(), tid, sid, reg, new_id(), new_id(), old)
    return dict(sensor=sid, registration=reg, snapshot=snap, job=job, attempt=attempt)
