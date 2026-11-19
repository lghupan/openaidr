import pytest
from psycopg import sql
from openaidr.server import data_lifecycle as life
from openaidr.server.store import new_id
from test_store import db as db
from test_data_lifecycle import configured as configured


def test_offboard_erases_global_metadata_and_blocks_restore(db, configured):
    cfg, register = configured
    db.create_user(cfg.tenant_id, 'synthetic@example.test', 'Test', 'hash', 'owner')
    check = life.offboard_plan(db, cfg)
    schema = check['plan']['schema']
    try:
        with pytest.raises(ValueError, match='inventory changed'):
            life.offboard(db, cfg, '0'*64, new_id())
        ident = new_id()
        result = life.offboard(db, cfg, check['sha256'], ident)
        assert result['state'] == 'primary_deleted'
        # Simulate a committed DROP whose external acknowledgement was lost.
        with register.connect() as remote:
            remote.execute("UPDATE lifecycle_tenants SET state='offboarding' WHERE tenant_id=%s",(cfg.tenant_id,))
            remote.execute('UPDATE lifecycle_requests SET applied_at=NULL WHERE request_id=%s',(ident,))
        assert life.offboard(db, cfg, check['sha256'], ident)['state'] == 'primary_deleted'
        assert db.scalar('SELECT to_regnamespace($1) IS NULL', schema)
        with register.locked(cfg.tenant_id, active=False) as (_, row):
            assert row['state'] == 'primary_deleted'
        with pytest.raises(ValueError, match='activation refused'):
            life.replay(db, cfg, restore=True)
    finally:
        # test_store fixture owns the synthetic schema and removes it on exit.
        with db.transaction() as conn:
            conn.execute('DROP SCHEMA IF EXISTS openaidr_erasure_receipts CASCADE')
            conn.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(sql.Identifier(schema)))


@pytest.mark.parametrize('wrong_database', [False, True])
def test_offboard_retry_requires_original_database_and_committed_receipt(db, configured, wrong_database):
    cfg, register = configured
    plan = life.offboard_plan(db, cfg)['plan']
    plan['schema'] = 'absent_' + new_id().replace('-', '')
    if wrong_database:
        plan['database'] += '_different'
    ident = new_id()
    with register.locked(cfg.tenant_id) as (remote, _):
        register.append(remote, cfg.tenant_id, ident, plan, 'operator')
        remote.execute("UPDATE lifecycle_tenants SET state='offboarding' WHERE tenant_id=%s", (cfg.tenant_id,))
    with pytest.raises(ValueError, match='database identity|erasure receipt'):
        life.offboard(db, cfg, life.digest(plan), ident)
    assert db.scalar('SELECT count(*) FROM tenants WHERE id=$1', cfg.tenant_id) == 1
    with register.locked(cfg.tenant_id, active=False) as (remote, row):
        assert row['state'] == 'offboarding'
        assert remote.execute('SELECT applied_at FROM lifecycle_requests WHERE request_id=%s',(ident,)).fetchone()['applied_at'] is None


def test_dedicated_offboard_refuses_single_tenant_control_plane(db, configured):
    cfg, _ = configured
    db.execute("INSERT INTO platform_admins(id,email,password_hash) VALUES($1,'staff@example.test','hash')", new_id())
    with pytest.raises(ValueError, match='Shared control-plane'):
        life.offboard_plan(db, cfg)
    assert db.scalar('SELECT count(*) FROM tenants') == 1
