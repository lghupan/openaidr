"""Synthetic telemetry without a private model-service dependency."""
from datetime import timedelta
from psycopg.types.json import Jsonb
from openaidr.server.store import now, new_id


def seed(st, sensor, age, *, pending=False):
    day = now().date() - timedelta(days=age)
    st.execute('SELECT ensure_telemetry_partitions($1,$2)', sensor['tenant_id'], day)
    stamp = now() - timedelta(days=age)
    ident = new_id()
    st.execute('''INSERT INTO events(id,tenant_id,sensor_id,time,kind,product,body,storage_day,received_at,risk)
        VALUES($1,$2,$3,$4,'sensor','',$5,$6,$4,$7)''', ident, sensor['tenant_id'], sensor['id'], stamp,
        Jsonb({'id': ident, 'kind': 'sensor'}), day, Jsonb({'status': 'pending' if pending else 'scored'}))
    st.set_detect_cursor(sensor['tenant_id'], {'time': now(), 'id': 'ffffffff-ffff-ffff-ffff-ffffffffffff'})
    return ident, day
