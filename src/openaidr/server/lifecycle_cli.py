"""Offline lifecycle operator commands (no workers or external publishers).

Run with ``python -m openaidr.server.lifecycle_cli``. Secrets use the ordinary
OPENAIDR_*_FILE environment support; never pass database credentials on argv.
"""
import argparse
import json
import sys

from .config import from_env
from .deletion_register import Register
from .store import Store
from . import data_lifecycle


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('init-register', 'register', 'restore-check'))
    args = parser.parse_args(argv)
    try:
        cfg = from_env()
        if args.operation == 'init-register':
            Register(cfg.deletion_register_url).initialize()
            result = {'state': 'register_initialized'}
        else:
            st = Store(cfg.database_url, db_pool_size=1, auth_pool_size=1)
            try:
                if args.operation == 'register':
                    data_lifecycle.register_tenant(st, cfg)
                    result = {'state': 'registered', 'tenant_id': cfg.tenant_id}
                elif args.operation == 'restore-check':
                    result = data_lifecycle.replay(st, cfg, restore=True)
            finally:
                st.close()
        print(json.dumps(result, default=str))
        return 0
    except Exception:
        # Connection errors can contain credentials/hostnames or restored data.
        print('Lifecycle operation failed; keep workloads stopped and inspect protected operator diagnostics.', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
