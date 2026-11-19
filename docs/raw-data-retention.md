# Raw data retention

New tenants default to 30 days. Existing policies, including an absent legacy
policy, are preserved. Expiry applies to telemetry, ingest/gateway inputs,
snapshot items, completed investigation transport and assessment inputs. Final
findings, results, review notes, security cases and account metadata remain.

Apply migration 0158 with the migration-owner role. It permits explicit raw-copy
expiry without weakening other immutability guards and keeps partition retirement
bounded. The API service runs raw cleanup independently of detection scheduling.
Unfinished processing can delay expiry and must be monitored. Expired snapshot
reads return 410, and scheduling cannot recreate investigations from expired input.
Age uses whole UTC storage days; disk pressure can retire younger completed days.

This prerequisite does not expose customer deletion or restore/offboarding tools.
Its dormant authority hooks become configurable only with the following backend
PR. Do not claim protection against old-snapshot resurrection until that gate and
independent authority have been deployed. Backup copies need separate expiry.

Validation: real PostgreSQL tests in `test_raw_retention.py` cover defaults,
raw/result separation, immutable notes, inline assessment results. Retention and ordinary-session compatibility tests use the
retained API harness. Host mount validation treats collector paths as Linux paths
on every test host. POSIX filesystem assertions still require Linux.

Roll back only to compatible code that understands expired snapshots; erasure is
irreversible. Restore testing must remain isolated until current deletion authority
is available. No production migrations, policy changes or deletions are performed.

Intermediate investigation payloads expire after settlement. Request accounting and
acknowledged finish receipts remain; uncertain calls or unacknowledged settlement
block payload expiry and restore activation until reconciled.
