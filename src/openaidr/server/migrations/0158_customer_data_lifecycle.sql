-- New tenants receive an explicit policy. Existing choices (including absence
-- of a policy) are deliberately not backfilled by a destructive migration.
CREATE FUNCTION default_raw_retention() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO tenant_telemetry_retention(tenant_id,mode) VALUES(NEW.id,'30');
    RETURN NEW;
END;
$$;
CREATE TRIGGER tenant_default_raw_retention AFTER INSERT ON tenants
    FOR EACH ROW EXECUTE FUNCTION default_raw_retention();

-- These are replay checkpoints, not the authoritative deletion register.
-- The authority lives outside the database's backup/restore boundary.
CREATE TABLE lifecycle_applied_requests (
    request_id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE lifecycle_tombstones (
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    kind text NOT NULL CHECK(kind IN ('finding','investigation','snapshot','case','finding_source')),
    object_key text NOT NULL,
    PRIMARY KEY(tenant_id,kind,object_key)
);
CREATE TABLE lifecycle_expired_snapshots (
    snapshot_id uuid PRIMARY KEY REFERENCES session_snapshots(id) ON DELETE CASCADE,
    expired_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE ordinary_hook_checks ADD COLUMN raw_expired_at timestamptz;
ALTER TABLE inline_guardrail_events ADD COLUMN raw_expired_at timestamptz;
CREATE OR REPLACE FUNCTION preserve_guardrail_feedback() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME='ordinary_hook_checks' THEN
      IF NEW.raw_expired_at IS NOT NULL
        AND NEW.assessment_input IS NULL AND NEW.assessment_input_sha256 IS NULL
        AND NEW.assessment_context IS NULL AND
        (to_jsonb(NEW)-ARRAY['assessment_input','assessment_input_sha256','assessment_context','raw_expired_at'])
        IS NOT DISTINCT FROM
        (to_jsonb(OLD)-ARRAY['assessment_input','assessment_input_sha256','assessment_context','raw_expired_at']) THEN
        RETURN NEW;
      END IF;
    END IF;
    IF TG_TABLE_NAME='ordinary_hook_review_revisions' THEN
      IF NEW.evidence_sha256 IS NULL AND
          (to_jsonb(NEW)-'evidence_sha256') IS NOT DISTINCT FROM (to_jsonb(OLD)-'evidence_sha256')
          AND EXISTS(SELECT 1 FROM ordinary_hook_checks WHERE id=OLD.check_id AND raw_expired_at IS NOT NULL) THEN
        RETURN NEW;
      END IF;
    END IF;
    RAISE EXCEPTION 'guardrail feedback evidence and revisions are immutable' USING ERRCODE='23514';
END;
$$;
ALTER TABLE ordinary_hook_checks ADD CONSTRAINT ordinary_expired_input CHECK (
    raw_expired_at IS NULL OR (assessment_input IS NULL AND assessment_input_sha256 IS NULL AND assessment_context IS NULL));
ALTER TABLE inline_guardrail_events ADD CONSTRAINT inline_expired_input CHECK (
    raw_expired_at IS NULL OR (target_body='{}'::jsonb AND captured_context='{}'::jsonb AND prepared_state='{}'::jsonb AND knowledge_snapshot IS NULL));
-- Expiring raw review evidence must not cascade-delete the customer's notes.
ALTER TABLE ordinary_hook_review_revisions DROP CONSTRAINT ordinary_hook_review_revisions_check_id_evidence_sha256_fkey;
ALTER TABLE ordinary_hook_review_revisions ADD FOREIGN KEY(check_id,evidence_sha256)
    REFERENCES ordinary_hook_review_evidence(check_id,sha256) ON DELETE SET NULL (evidence_sha256);

CREATE OR REPLACE FUNCTION guardrail_assessment_history_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status<>'pending' AND NEW.prepared_state='{}'::jsonb AND NEW.knowledge_snapshot IS NULL
        AND (to_jsonb(NEW)-ARRAY['prepared_state','knowledge_snapshot']) IS NOT DISTINCT FROM
            (to_jsonb(OLD)-ARRAY['prepared_state','knowledge_snapshot'])
        AND EXISTS(SELECT 1 FROM inline_guardrail_events WHERE id=OLD.guardrail_event_id AND raw_expired_at IS NOT NULL) THEN
        RETURN NEW;
    END IF;
    IF ROW(NEW.guardrail_event_id,NEW.generation,NEW.tenant_id,NEW.created_at)
       IS DISTINCT FROM ROW(OLD.guardrail_event_id,OLD.generation,OLD.tenant_id,OLD.created_at)
       OR (OLD.status <> 'pending' AND NEW IS DISTINCT FROM OLD)
       OR (OLD.context_pinned AND ROW(NEW.context_pinned,NEW.prepared_state,
           NEW.knowledge_snapshot,NEW.context_revision,NEW.scope_key,NEW.scope_version)
           IS DISTINCT FROM ROW(OLD.context_pinned,OLD.prepared_state,
           OLD.knowledge_snapshot,OLD.context_revision,OLD.scope_key,OLD.scope_version)) THEN
        RAISE EXCEPTION 'assessment generation is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;

-- Preserve sealed headers/results but allow deletion of raw items after expiry.
-- Updates remain forbidden; expiry is explicit and cannot unseal a snapshot.
CREATE OR REPLACE FUNCTION preserve_session_snapshot_item() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' AND EXISTS (
        SELECT 1 FROM lifecycle_expired_snapshots WHERE snapshot_id=OLD.snapshot_id
    ) THEN RETURN OLD; END IF;
    IF TG_OP='UPDATE' OR EXISTS (
        SELECT 1 FROM session_snapshots WHERE id=CASE WHEN TG_OP='DELETE' THEN OLD.snapshot_id ELSE NEW.snapshot_id END
            AND sealed_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'session snapshot item is immutable' USING ERRCODE='23514';
    END IF;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;

-- Prevent replay/retry from recreating erased knowledge with a new row ID.
CREATE FUNCTION suppress_erased_knowledge() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE blocked boolean;
BEGIN
    IF TG_TABLE_NAME='findings' THEN
        SELECT EXISTS(SELECT 1 FROM lifecycle_tombstones WHERE tenant_id=NEW.tenant_id AND
            ((kind='finding' AND object_key=NEW.id::text)
             OR (kind='finding_source' AND object_key=NEW.rule_id || ':' || NEW.primary_event_id::text)
             OR (kind='investigation' AND object_key=NEW.investigation_job_id::text)
             OR (kind='snapshot' AND object_key=NEW.snapshot_id::text))) INTO blocked;
    ELSIF TG_TABLE_NAME='session_investigation_jobs' THEN
        SELECT EXISTS(SELECT 1 FROM lifecycle_tombstones WHERE tenant_id=NEW.tenant_id AND
            ((kind='investigation' AND object_key=NEW.id::text)
             OR (kind='snapshot' AND object_key=NEW.snapshot_id::text)))
            OR EXISTS(SELECT 1 FROM lifecycle_expired_snapshots WHERE snapshot_id=NEW.snapshot_id) INTO blocked;
    ELSE
        SELECT EXISTS(SELECT 1 FROM lifecycle_tombstones WHERE tenant_id=NEW.tenant_id
            AND kind='case' AND object_key=NEW.id::text) INTO blocked;
    END IF;
    IF blocked THEN RETURN NULL; END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER lifecycle_findings BEFORE INSERT OR UPDATE ON findings
    FOR EACH ROW EXECUTE FUNCTION suppress_erased_knowledge();
CREATE TRIGGER lifecycle_investigations BEFORE INSERT OR UPDATE ON session_investigation_jobs
    FOR EACH ROW EXECUTE FUNCTION suppress_erased_knowledge();
CREATE TRIGGER lifecycle_cases BEFORE INSERT OR UPDATE ON risk_cases
    FOR EACH ROW EXECUTE FUNCTION suppress_erased_knowledge();

-- An in-flight summary must not republish sources captured before an erasure.
CREATE FUNCTION suppress_stale_lifecycle_cache() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE captured timestamptz := transaction_timestamp();
BEGIN
    IF TG_TABLE_NAME='overview_summaries' THEN
        captured := LEAST(captured,(NEW.summary->'sources'->0->>'captured_at')::timestamptz);
    END IF;
    IF EXISTS(SELECT 1 FROM lifecycle_applied_requests WHERE tenant_id=NEW.tenant_id AND applied_at>captured) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER lifecycle_overview_cache BEFORE INSERT OR UPDATE ON overview_summaries
    FOR EACH ROW EXECUTE FUNCTION suppress_stale_lifecycle_cache();
CREATE TRIGGER lifecycle_report_cache BEFORE INSERT ON summary_report_outbox
    FOR EACH ROW EXECUTE FUNCTION suppress_stale_lifecycle_cache();
CREATE TRIGGER lifecycle_slack_cache BEFORE INSERT ON slack_outbox
    FOR EACH ROW EXECUTE FUNCTION suppress_stale_lifecycle_cache();

-- Runtime keeps ordinary audit rows append-only. Privacy erasure has a narrow
-- exception, bound to a recorded request and its object tombstones, and is
-- itself durably recorded outside this database before invocation.
CREATE FUNCTION erase_lifecycle_audit(p_tenant uuid,p_request uuid,p_targets uuid[])
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE removed bigint;
BEGIN
    IF NOT EXISTS(SELECT 1 FROM lifecycle_applied_requests WHERE request_id=p_request AND tenant_id=p_tenant)
        OR EXISTS(SELECT 1 FROM unnest(p_targets) id WHERE NOT EXISTS (
            SELECT 1 FROM lifecycle_tombstones t WHERE t.tenant_id=p_tenant
                AND t.kind IN ('finding','investigation','case') AND t.object_key=id::text)) THEN
        RAISE EXCEPTION 'recorded lifecycle scope required' USING ERRCODE='42501';
    END IF;
    DELETE FROM audit_log WHERE tenant_id=p_tenant AND target_id=ANY(p_targets::text[]);
    GET DIAGNOSTICS removed = ROW_COUNT;
    RETURN removed;
END;
$$;
-- Resolve tables in the trusted installation schema; never through pg_temp.
DO $$ BEGIN
    EXECUTE format('ALTER FUNCTION erase_lifecycle_audit(uuid,uuid,uuid[]) SET search_path=%I,pg_temp',current_schema());
END $$;
REVOKE ALL ON FUNCTION erase_lifecycle_audit(uuid,uuid,uuid[]) FROM PUBLIC;

-- Keep step/request accounting identities while expiring intermediate raw content.
CREATE FUNCTION preserve_lifecycle_step() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.input_manifest='{}'::jsonb AND COALESCE(NEW.result,'{}'::jsonb)='{}'::jsonb
        AND (to_jsonb(NEW)-ARRAY['input_manifest','result']) IS NOT DISTINCT FROM
            (to_jsonb(OLD)-ARRAY['input_manifest','result'])
        AND EXISTS(SELECT 1 FROM session_investigation_attempts a
            JOIN session_investigation_jobs j ON j.id=a.job_id
            JOIN lifecycle_expired_snapshots e ON e.snapshot_id=j.snapshot_id WHERE a.id=OLD.attempt_id) THEN
        RETURN NEW;
    END IF;
    IF ROW(NEW.id,NEW.attempt_id,NEW.ordinal,NEW.kind,NEW.input_manifest,NEW.input_sha256,NEW.input_bytes)
        IS DISTINCT FROM ROW(OLD.id,OLD.attempt_id,OLD.ordinal,OLD.kind,OLD.input_manifest,OLD.input_sha256,OLD.input_bytes)
        OR (OLD.state<>'prepared' AND NEW IS DISTINCT FROM OLD) THEN
        RAISE EXCEPTION 'investigation step is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER investigation_step_guard ON session_investigation_steps;
CREATE TRIGGER investigation_step_guard BEFORE UPDATE ON session_investigation_steps
    FOR EACH ROW EXECUTE FUNCTION preserve_lifecycle_step();
