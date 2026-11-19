# Customer data lifecycle

Raw data has a time limit. Retained security knowledge and account metadata do
not inherit that limit. This implementation does not create fictitious customer
requests: the request register starts empty.

| Class | Retention and removal |
| --- | --- |
| Native events and telemetry exports | Existing tenant policy and bounded partition removal |
| Ingest evidence, gateway request payloads, session receipts | Tenant raw policy |
| Sealed snapshot items; completed investigation submissions, steps, reads and remote transport copies | Tenant raw policy; snapshot headers and final job/attempt results remain |
| Ordinary hook assessment input and review evidence; inline assessment inputs | Tenant raw policy; decision, review label and customer note remain |
| Findings, investigation results, security cases and notes | Explicit customer deletion or tenant offboarding |
| Knowledge, configuration, users, email, billing and credits | Kept until explicitly removed or tenant offboarding |
| Recovery copies | Independently bounded recovery window and purge; current erasure authority reapplied before activation |

New tenants default to 30 days. Existing tenants are not silently backfilled;
an existing missing policy must be resolved explicitly before registration.
Existing 7/30/90-day, unlimited and disk-pressure settings remain compatible.
The raw worker runs with the API service, independently of detection scheduling.
Age boundaries use UTC storage days. Copy timestamps currently bound retained
snapshot and investigation inputs; review the source-data timeline when changing
a customer's contractual definition of age. A running investigation or pending
remote submission can block expiry. It is never silently treated as deleted.

## Independent authority

Provision a **separate PostgreSQL database outside the tenant's recovery
boundary**, with its own backup and access policy. Use a separate register and
credential per tenant, or enforce equivalent tenant isolation at the database
layer. Do not give a tenant backend an unrestricted shared control-plane DB
credential. The runtime rejects a register pointing at its own database, but
physical independence and backup scope are deployment responsibilities.

Set `OPENAIDR_DELETION_REGISTER_URL_FILE` to a protected file containing that
database's connection string. The file is not part of the tenant backup. Use
private connectivity and verified TLS for remote database connections.

Initialize the empty register with an operator credential, migrate the tenant
database, bind `OPENAIDR_TENANT_ID`, and register the existing explicit policy:

```sh
python -m openaidr.server.lifecycle_cli init-register
openaidr-server migrate
python -m openaidr.server.lifecycle_cli register
```

Registration is an explicit one-time operation, **never** an automatic fallback
on restore. The register retains policy, the latest irreversible raw deletion
cutoff, UUIDs/selectors, request timestamps, an opaque actor ID and application
status. It stores no finding text, prompts, email addresses or free-text reasons.
Its UUID tombstones must outlive every recoverable tenant copy. Protect it from
tenant restore and unauthorized changes. Losing it blocks activation; restore
its independently protected current state rather than registering an old tenant
snapshot as a new authority.

Once configured, a register failure prevents deletion/policy edits and startup
reconciliation. A running service logs deferred maintenance; monitor these
failures and retry. Do not remove the setting to bypass a restore failure.
Unconfigured self-hosted installations retain ordinary raw expiry, but the new
on-demand API is disabled and they do not provide the external restore guarantee.

## Customer deletion API

The existing authenticated, tenant-scoped console API and CSRF checks apply.
Only `it_admin` can preview, submit or list requests.

1. `POST /api/v1/data-deletions/preview` with
   `{"kind":"findings","ids":["<uuid>"]}`. Other kinds are `investigations`
   and `cases`. The response includes a concrete plan and `sha256`.
2. Display the entire plan. Deleting an investigation finding also removes its
   parent investigation, other findings for that snapshot, affected cases and
   review revisions. This avoids leaving a second copy of the deleted finding.
   Deleting a case alone removes that case and its notes/links; independently
   retained investigation results remain. Running investigations return 409.
3. `POST /api/v1/data-deletions` with the same kind/IDs, a new `request_id` UUID,
   and the preview `sha256`. Changed scope returns 409 and needs a new preview.
4. Retry with the same request ID and scope after an interrupted response.
   `GET /api/v1/data-deletions` lists the most recent 100 durable requests.

Intent commits in the independent register before local deletion. Local failure
leaves a durable pending request which the worker retries. Local writes and
publishers are serialized during erasure, and tombstones suppress regenerated
findings/jobs. Copied dashboard summaries, activity caches, local reports and
notification outboxes are invalidated. Shared case feedback enters the existing
remote deletion queue.

`primary_deleted` / `applied_at` means local application data was erased. It is
**not** a claim that old backups, sent Slack messages, exported reports, remote
Intelligence requests or other processor copies are already erased. Those need
provider expiry or verified deletion receipts. `external_completed_at` remains
unset until that separate verification is integrated; do not report a request
as fully complete from `applied_at` alone. Previously delivered customer-owned
exports cannot be remotely retracted by this backend.

## Restore gate

Keep ingress and workers stopped after database restore/WAL replay. Run the
current backend migrations offline, then:

```sh
python -m openaidr.server.lifecycle_cli restore-check
```

The command reads current external authority, rejects offboarded/unregistered
tenants, reapplies deletions regardless of old local checkpoints, reconciles the
current policy and earlier raw deletion cutoff, and drains expired raw data.
It exits nonzero if authority is unavailable, expiry is blocked or cleanup is
incomplete. Resolve unfinished work in an isolated recovery environment and
rerun; do not serve that database. Normal lifespan startup repeats the same gate
before starting any workers when the register is configured.

WAL includes committed database deletes, but WAL recovery to an earlier point
cannot know later requests. The external register supplies those later decisions.
Individual customer requests do not rewrite immutable backup snapshots. Copies
remain inaccessible for ordinary use and expire under the recovery policy.

## Validation and rollout

This patch targets the public checkout through migration 0158. The hosted private
backend has a newer migration sequence and additional storage. Port the change
upstream, allocate its next migration number, inventory those additional raw
stores, and update the explicit schema contract before a hosted release. The
restore gate refuses newer or differently named schemas rather than silently
declaring unexamined stores clean. This public patch alone is not a hosted rollout.
The audit erasure helper allows only recorded request/tombstone scopes; ordinary
runtime audit writes remain append-only. Offboarding uses the maintenance-owner
database credential, never a broader privilege grant to the running application.

Run `tests/test_data_lifecycle.py` with `OPENAIDR_TEST_DATABASE_URL` and a **different**
`OPENAIDR_TEST_REGISTER_URL`. The tests use synthetic tenants and disposable
schemas, including old-snapshot replay, failure after durable intent, cross-tenant
denial, CSRF/roles, raw/result separation, notes preservation and offboarding.

Before production: provision isolated authority and connectivity; approve legacy
policies; apply migrations; register each tenant; perform an isolated restore
drill; deploy the startup gate; verify raw expiry and external backup inventories.
Infrastructure restore tools require a fresh authority credential and run the
gate before activating services. An old signed image lacking this command must
remain isolated until a compatible, authorized recovery image is prepared.

Deletion cannot be rolled back by removing this migration. Roll back application
code only to a version that still honors deletion authority, or keep the tenant
stopped. Any recovery from an older backup must pass the current gate again.

Local validation on 2026-11-18 used disposable PostgreSQL 16 databases: 17 lifecycle,
25 store and 16 ingest-evidence tests passed. Public dependency and publication
checks passed. The older retention/overview suites require the unbundled private
Intelligence service and were not validated here. These are synthetic tests,
not customer deletion receipts or evidence of a production deployment.
