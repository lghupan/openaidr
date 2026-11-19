"""Minimal external deletion authority; never include this DB in tenant restores.

Provision a separate PostgreSQL database and credential. Initialization is an
explicit operator command. The runtime never creates authority from a restored
tenant's possibly stale contents. Intents commit BEFORE deleting local data.
"""
from contextlib import contextmanager
import json

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


DDL = """
CREATE TABLE IF NOT EXISTS lifecycle_tenants (
    tenant_id uuid PRIMARY KEY,
    policy jsonb NOT NULL,
    raw_deleted_through date,
    state text NOT NULL DEFAULT 'active' CHECK(state IN ('active','offboarding','primary_deleted','complete')),
    revision bigint NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS lifecycle_requests (
    request_id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES lifecycle_tenants(tenant_id),
    plan jsonb NOT NULL,
    actor_id text NOT NULL,
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    applied_at timestamptz,
    external_completed_at timestamptz
);
CREATE INDEX IF NOT EXISTS lifecycle_requests_tenant ON lifecycle_requests(tenant_id,requested_at,request_id);
"""


class Register:
    def __init__(self, url):
        if not url:
            raise ValueError('An independent deletion register is required')
        self.url = url

    def connect(self):
        conn = psycopg.connect(self.url, row_factory=dict_row, connect_timeout=5)
        conn.execute("SET statement_timeout='15s'; SET lock_timeout='5s'")
        conn.commit()
        return conn

    def initialize(self):
        with self.connect() as conn:
            conn.execute(DDL)

    def register(self, tenant, policy):
        with self.connect() as conn:
            conn.execute('INSERT INTO lifecycle_tenants(tenant_id,policy) VALUES(%s,%s)', (tenant, Jsonb(policy)))

    @contextmanager
    def locked(self, tenant, *, active=True):
        with self.connect() as conn:
            # Session lock survives the intent commit, fencing restore, policy
            # changes and deletion against each other without distributed 2PC.
            conn.execute('SELECT pg_advisory_lock(hashtextextended(%s,0))', ('lifecycle:' + tenant,))
            conn.commit()
            row = conn.execute('SELECT * FROM lifecycle_tenants WHERE tenant_id=%s', (tenant,)).fetchone()
            if row is None or (active and row['state'] != 'active'):
                raise ValueError('Tenant is unregistered or offboarded; activation refused')
            yield conn, row

    @staticmethod
    def append(conn, tenant, request_id, plan, actor):
        existing = conn.execute('SELECT tenant_id,plan FROM lifecycle_requests WHERE request_id=%s',
                                (request_id,)).fetchone()
        if existing:
            if str(existing['tenant_id']) != tenant or existing['plan'] != plan:
                raise ValueError('Deletion request ID already has a different scope')
            return
        conn.execute('INSERT INTO lifecycle_requests(request_id,tenant_id,plan,actor_id) VALUES(%s,%s,%s,%s)',
                     (request_id, tenant, Jsonb(plan), actor))
        conn.execute('UPDATE lifecycle_tenants SET revision=revision+1,updated_at=clock_timestamp() WHERE tenant_id=%s', (tenant,))
        conn.commit()

    @staticmethod
    def policy(conn, tenant, policy):
        # An intended shorter policy remains authoritative if the tenant write
        # fails; reconciliation repairs it before another edit/activation.
        conn.execute('UPDATE lifecycle_tenants SET policy=%s,revision=revision+1,updated_at=clock_timestamp() WHERE tenant_id=%s',
                     (Jsonb(policy), tenant))
        conn.commit()

    @staticmethod
    def raw_deleted(conn, tenant, day):
        conn.execute('''UPDATE lifecycle_tenants SET raw_deleted_through=GREATEST(raw_deleted_through,%s),
            revision=revision+1,updated_at=clock_timestamp() WHERE tenant_id=%s
            AND (raw_deleted_through IS NULL OR raw_deleted_through<%s)''', (day, tenant, day))
        conn.commit()

    def requests(self, tenant):
        with self.connect() as conn:
            rows = conn.execute('''SELECT request_id,plan,requested_at,applied_at,external_completed_at
                FROM lifecycle_requests WHERE tenant_id=%s ORDER BY requested_at DESC LIMIT 100''', (tenant,)).fetchall()
            return json.loads(json.dumps(rows, default=str))
