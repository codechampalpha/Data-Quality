# In-House Data Quality Platform — Target Architecture & Design

**Replacing Experian Data Studio | React + Python + PostgreSQL on OpenShift**
Version 1.0 — Draft for review

---

## 1. Solution Overview

The platform is an internal data quality rule engine. Business users define rules in plain English; the technical team codes them as Python expressions, tests them in UAT, and promotes them to Approved status. Workflows map approved rules to columns of a registered dataset. Each day, when the source file lands on the NDM shared path (or on manual trigger), the engine assembles the approved rule code in memory, executes it against the dataset with pandas, and writes a wide output table containing the original columns plus one Pass/Fail column per rule-column mapping. A second layer aggregates rule results into weighted groups and stores percentage scores. Output is retained on a rolling 90-day window (monthly feeds keep their monthly batches), viewable and downloadable through the React application under role-based access control.

### 1.1 Component architecture (OpenShift)

```
┌────────────────────────── OpenShift Project ──────────────────────────┐
│                                                                       │
│  ┌─────────────┐   ┌──────────────────┐   ┌────────────────────────┐  │
│  │  React SPA  │──▶│  API Service     │──▶│  PostgreSQL 15+        │  │
│  │  (nginx pod)│   │  FastAPI/uvicorn │   │  (PVC-backed,          │  │
│  └─────────────┘   │  - REST + auth   │   │   encrypted storage)   │  │
│                    │  - RBAC          │   │  schemas:              │  │
│                    └──────────────────┘   │   dq_meta  (app tables)│  │
│                    ┌──────────────────┐   │   dq_output (per-wf    │  │
│                    │  Worker Service  │──▶│     partitioned tables)│  │
│                    │  (job dispatcher │   │   dq_ref   (lookups)   │  │
│                    │   + rule engine) │   └────────────────────────┘  │
│                    └────────┬─────────┘                               │
│                             │ read-only mount / polling               │
│                    ┌────────▼─────────┐                               │
│                    │ NDM shared path  │  daily CSV + count file       │
│                    └──────────────────┘  (source: Big Data / PySpark) │
└───────────────────────────────────────────────────────────────────────┘
```

Three deployments: the React frontend (static, served by nginx), the API service (FastAPI), and the Worker service (job dispatcher + rule engine). Separating the worker from the API means a long-running 500K-row job never blocks user requests, and the worker can later be scaled to multiple replicas for parallel workflows without touching the API. Both API and worker share the same PostgreSQL instance; the worker also has read access to the NDM shared path (mounted as a volume or reachable via network share).

### 1.2 Technology stack (all open source, Artifactory-friendly)

| Layer | Choice | Notes |
|---|---|---|
| Database | PostgreSQL 15+ | Native declarative partitioning (critical for 90-day retention), JSONB for flexible metadata, 1600-column limit comfortably covers wide outputs |
| Backend API | Flask 3.0 + Flask-CORS 4.0 | **Confirmed in Artifactory and already in use** — keep the existing framework; no rewrite value in switching |
| Auth | ldap3 2.9 + PyJWT 2.9 + cryptography 43 | Corporate directory authentication → short-lived JWT with username/roles/workspace claims → Flask decorator enforces RBAC per request |
| DB access | SQLAlchemy (core) + psycopg2 | Confirmed available; psycopg2 `copy_expert` is the key to fast bulk writes |
| Engine | pandas 2.2 + numpy | Already in use; all 150 rules are already written in pandas idiom. Worker is a plain Python process (no web framework) |
| Excel support | openpyxl 3.1 / xlrd 2.0 | Already in use — keeps the reference-table upload wizard accepting .xlsx alongside CSV |
| Job queue | PostgreSQL-backed queue (own table) | No Redis/Celery dependency; see §5.3 |
| Frontend | React (existing) + polling for progress | Reuse existing sidebar/wizard shell |

All packages confirmed available in Artifactory. Identity strategy: LDAP for authentication (who you are), the `user_role` table for authorization (what you may do) — so Approver/Reviewer assignments remain a business decision inside the app rather than an AD group ticket.

### 1.3 Sizing reality check

Per daily workflow: 500K rows × (~250 original columns + up to ~200 result columns). Result columns stored as `CHAR(1)` (`P`/`F`, rendered as Pass/Fail in the UI) keep the result payload tiny (~200 bytes/row). The dominant cost is the original text columns — estimate 1–2 KB/row, so roughly 0.5–1 GB per day per large workflow and 45–90 GB per workflow over 90 days. With ~25 datasets (mixed daily/monthly), plan **1.5–2.5 TB** of PostgreSQL storage with headroom. In-memory processing of one batch (500K × 250 cols as pandas strings) fits easily in 8–16 GB; the 128 GB ceiling allows 2–3 concurrent workflow runs safely.

---

## 2. Database Design (schema `dq_meta`)

Fresh design; the demo SQLite tables are treated as intent, not structure. Naming: snake_case, singular table names, `*_id` surrogate keys.

### 2.1 Rules — definition, versioning, lookups

The core change from the demo: **a rule's identity is separated from its code**. `rule` is the stable business object; `rule_version` carries the Python expression and moves through the approval lifecycle. Only one Approved version per rule can exist (enforced by a partial unique index), which implements your "newer version supersedes, old one retired" requirement cleanly.

```sql
CREATE TABLE rule_category (
    rule_type_id     SERIAL PRIMARY KEY,
    rule_type        TEXT NOT NULL UNIQUE,          -- e.g. Validity, Completeness, Conformity, Lookup
    rule_type_desc   TEXT
);

CREATE TABLE rule (
    rule_id          SERIAL PRIMARY KEY,
    rule_code        TEXT NOT NULL UNIQUE,          -- short stable code, e.g. 'R101' — used in output column names
    rule_name        TEXT NOT NULL UNIQUE,          -- e.g. 'City Is Valid Check'
    rule_type_id     INT  REFERENCES rule_category(rule_type_id),
    business_definition TEXT NOT NULL,              -- step 1: plain-English rule from business, with examples
    expected_data_type  TEXT,                       -- advisory: string/int/date...
    lob              TEXT,
    workspace        TEXT NOT NULL,
    created_by       TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_active        BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE rule_version (
    rule_version_id  SERIAL PRIMARY KEY,
    rule_id          INT NOT NULL REFERENCES rule(rule_id),
    version_no       INT NOT NULL,
    python_expression TEXT NOT NULL,                -- body of the rule function (contract in §5.1)
    parameter_schema JSONB NOT NULL DEFAULT '{}',   -- declares params the rule accepts + defaults
    required_columns JSONB NOT NULL DEFAULT '[]',   -- secondary columns the rule reads (e.g. mname), validated pre-run
    status           TEXT NOT NULL DEFAULT 'Draft', -- Draft → InReview → Approved(UAT) → Approved → Retired / Rejected
    status_history   JSONB NOT NULL DEFAULT '[]',   -- [{status, by, at, comment}] — audit trail of the 4-step process
    reviewed_by      TEXT,
    approved_by      TEXT,
    approved_at      TIMESTAMPTZ,
    retired_at       TIMESTAMPTZ,
    comments         TEXT,
    created_by       TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (rule_id, version_no),
    CHECK (status IN ('Draft','InReview','ApprovedUAT','Approved','Retired','Rejected'))
);

-- exactly one production-approved version per rule
CREATE UNIQUE INDEX ux_rule_one_approved
    ON rule_version(rule_id) WHERE status = 'Approved';

-- one rule ↔ many lookup tables; each lookup bound to one column of the lookup dataset
CREATE TABLE rule_lookup_binding (
    binding_id       SERIAL PRIMARY KEY,
    rule_version_id  INT NOT NULL REFERENCES rule_version(rule_version_id) ON DELETE CASCADE,
    lookup_alias     TEXT NOT NULL,                 -- name the rule code uses, e.g. 'valid_states'
    ref_dataset_id   INT NOT NULL REFERENCES ref_dataset(ref_dataset_id),
    lookup_column    TEXT NOT NULL,                 -- which column of the lookup table feeds the rule
    UNIQUE (rule_version_id, lookup_alias)
);
```

### 2.2 Reference (lookup) datasets — wizard-created, table-per-dataset

Your existing upload wizard behavior is kept (user names the table and columns, data lands in a dedicated physical table in schema `dq_ref`), but every wizard upload must register itself here so rules can bind to it:

```sql
CREATE TABLE ref_dataset (
    ref_dataset_id   SERIAL PRIMARY KEY,
    name             TEXT NOT NULL UNIQUE,          -- business name, e.g. 'USA State Codes'
    physical_table   TEXT NOT NULL UNIQUE,          -- e.g. 'dq_ref.usa_states'
    description      TEXT,
    columns_json     JSONB NOT NULL,                -- [{name, data_type}]
    source_file      TEXT,
    row_count        INT NOT NULL DEFAULT 0,
    workspace        TEXT NOT NULL,
    uploaded_by      TEXT NOT NULL,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_active        BOOLEAN NOT NULL DEFAULT TRUE
);
```

Re-uploading a lookup replaces the physical table contents and bumps `loaded_at` — rules pick up new values on the next run automatically.

### 2.3 Input datasets and daily batches

Input files are **not** persisted separately in PostgreSQL — the workflow output already contains every original column, so a raw copy would double storage for no benefit. The dataset registry describes the contract (expected columns, source path, count-file pattern); each arrival is tracked as a batch.

```sql
CREATE TABLE dataset (
    dataset_id       SERIAL PRIMARY KEY,
    dataset_name     TEXT NOT NULL UNIQUE,
    lob              TEXT NOT NULL,
    description      TEXT,
    schedule_type    TEXT NOT NULL DEFAULT 'daily', -- daily | monthly
    source_type      TEXT NOT NULL DEFAULT 'ndm',   -- ndm | impala (future direct connect)
    source_path      TEXT,                          -- NDM landing folder
    file_pattern     TEXT,                          -- e.g. 'ACCT_MASTER_{YYYYMMDD}.csv'
    count_file_pattern TEXT,                        -- e.g. 'ACCT_MASTER_{YYYYMMDD}.cnt'
    delimiter        TEXT NOT NULL DEFAULT ',',
    retention_batches INT NOT NULL DEFAULT 90,      -- 90 daily batches, or e.g. 3 for monthly feeds
    workspace        TEXT NOT NULL,
    created_by       TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_active        BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE dataset_column (
    dataset_id       INT NOT NULL REFERENCES dataset(dataset_id) ON DELETE CASCADE,
    column_name      TEXT NOT NULL,
    ordinal_position INT NOT NULL,
    data_type        TEXT NOT NULL DEFAULT 'text',
    PRIMARY KEY (dataset_id, column_name)
);

CREATE TABLE dataset_batch (
    batch_id         SERIAL PRIMARY KEY,
    dataset_id       INT NOT NULL REFERENCES dataset(dataset_id),
    batch_date       DATE NOT NULL,                 -- business date of the file
    file_name        TEXT NOT NULL,
    expected_count   BIGINT,                        -- from the .cnt file
    actual_count     BIGINT,
    load_status      TEXT NOT NULL DEFAULT 'Detected',
    detected_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (dataset_id, batch_date),                -- same-day re-arrival = same batch, overwrite semantics
    CHECK (load_status IN ('Detected','CountMismatch','Ready','Processed','Failed'))
);
```

**rec_id generation (Q7):** the engine assigns `rec_id = (batch_id × 10^9) + row_number` — a deterministic BIGINT that is unique across all batches, sortable, and lets you trace any output row back to its batch and original file line without a lookup. A same-day overwrite reuses the batch, truncates the day's partition, and regenerates identical rec_ids, keeping re-runs idempotent.

### 2.4 Workflows — mappings, parameters, groups

```sql
CREATE TABLE workflow (
    workflow_id      SERIAL PRIMARY KEY,
    workflow_name    TEXT NOT NULL UNIQUE,
    description      TEXT,
    dataset_id       INT NOT NULL REFERENCES dataset(dataset_id),
    output_table     TEXT NOT NULL UNIQUE,          -- e.g. 'dq_output.wf_12_acct_master'
    trigger_mode     TEXT NOT NULL DEFAULT 'auto',  -- auto (on file arrival) | manual
    status           TEXT NOT NULL DEFAULT 'Draft', -- Draft | Active | Inactive
    workspace        TEXT NOT NULL,
    user_class       TEXT,
    created_by       TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by       TEXT,
    updated_at       TIMESTAMPTZ
);

-- one row per (rule, column) pair; order defines output column order (Q19)
CREATE TABLE workflow_rule_mapping (
    mapping_id       SERIAL PRIMARY KEY,
    workflow_id      INT NOT NULL REFERENCES workflow(workflow_id) ON DELETE CASCADE,
    rule_id          INT NOT NULL REFERENCES rule(rule_id),        -- version resolved at run time = current Approved
    column_name      TEXT NOT NULL,
    display_order    INT NOT NULL,
    parameters       JSONB NOT NULL DEFAULT '{}',   -- per-mapping values for the rule's parameter_schema (Q14)
    result_column    TEXT NOT NULL,                 -- e.g. 'R101_city' — physical output column name
    is_active        BOOLEAN NOT NULL DEFAULT TRUE,
    added_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workflow_id, rule_id, column_name)
);

-- layer 2: weighted rule groups with per-workflow thresholds (Q18, confirmed)
CREATE TABLE workflow_rule_group (
    group_id         SERIAL PRIMARY KEY,
    workflow_id      INT NOT NULL REFERENCES workflow(workflow_id) ON DELETE CASCADE,
    group_name       TEXT NOT NULL,
    display_order    INT NOT NULL,
    pass_threshold   NUMERIC(6,3) NOT NULL DEFAULT 99.0,  -- pass_rate ≥ this → Pass
    fail_threshold   NUMERIC(6,3) NOT NULL DEFAULT 85.0,  -- pass_rate < this → Fail; between = Warning
    use_weighted     BOOLEAN NOT NULL DEFAULT TRUE,       -- which rate the thresholds apply to
    UNIQUE (workflow_id, group_name),
    CHECK (pass_threshold >= fail_threshold)
);

CREATE TABLE workflow_group_member (
    group_id         INT NOT NULL REFERENCES workflow_rule_group(group_id) ON DELETE CASCADE,
    mapping_id       INT NOT NULL REFERENCES workflow_rule_mapping(mapping_id) ON DELETE CASCADE,
    weight           NUMERIC NOT NULL DEFAULT 1,    -- coefficient applied to this mapping inside the group
    PRIMARY KEY (group_id, mapping_id)
);
```

Group score computed per run: `weighted_pass_rate = Σ(weight × passed_rows) / Σ(weight × total_rows)` alongside the unweighted pass rate. The group's **status** is then derived from per-workflow thresholds (confirmed model): pass rate ≥ `pass_threshold` → **Pass**, below `fail_threshold` → **Fail**, in between → **Warning**. Example: Pass at ≥99.0%, Fail below 85.0%, Warning in the 85–99 band. Thresholds are set per group during workflow design, so different workflows carry different tolerances without code changes.

### 2.5 Runs, results summaries, and the job queue

```sql
CREATE TABLE workflow_run (
    run_id           SERIAL PRIMARY KEY,
    workflow_id      INT NOT NULL REFERENCES workflow(workflow_id),
    batch_id         INT NOT NULL REFERENCES dataset_batch(batch_id),
    batch_date       DATE NOT NULL,
    trigger_type     TEXT NOT NULL,                 -- auto | manual
    triggered_by     TEXT NOT NULL,                 -- username or 'system'
    status           TEXT NOT NULL DEFAULT 'Queued',
    queued_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    total_rows       BIGINT DEFAULT 0,
    progress_pct     NUMERIC(5,2) DEFAULT 0,        -- driven by engine phases, feeds the UI progress bar
    current_step     TEXT,                          -- e.g. 'Executing rule 7/23: City Is Valid Check'
    rule_versions_used JSONB,                       -- {rule_id: rule_version_id} snapshot — audit: exactly what ran
    engine_log       TEXT,                          -- generated code + execution log for review (Q27)
    error_message    TEXT,
    CHECK (status IN ('Queued','Running','Completed','Failed','Cancelled'))
);

-- prevent overlapping runs of the same workflow (Q21)
CREATE UNIQUE INDEX ux_one_active_run_per_wf
    ON workflow_run(workflow_id) WHERE status IN ('Queued','Running');

-- per-run aggregates (replaces demo wf1_validation_results_by_rule — one set of generic tables, not one per workflow)
CREATE TABLE run_rule_summary (
    run_id           INT NOT NULL REFERENCES workflow_run(run_id) ON DELETE CASCADE,
    mapping_id       INT NOT NULL,
    rule_name        TEXT NOT NULL,
    column_name      TEXT NOT NULL,
    passed_rows      BIGINT NOT NULL,
    failed_rows      BIGINT NOT NULL,
    pass_rate        NUMERIC(7,4) NOT NULL,
    PRIMARY KEY (run_id, mapping_id)
);

CREATE TABLE run_group_summary (
    run_id           INT NOT NULL REFERENCES workflow_run(run_id) ON DELETE CASCADE,
    group_id         INT NOT NULL,
    group_name       TEXT NOT NULL,
    passed_rows      BIGINT NOT NULL,
    failed_rows      BIGINT NOT NULL,
    pass_rate        NUMERIC(7,4) NOT NULL,
    weighted_pass_rate NUMERIC(7,4) NOT NULL,
    group_status     TEXT NOT NULL,              -- Pass | Warning | Fail, derived from group thresholds
    PRIMARY KEY (run_id, group_id),
    CHECK (group_status IN ('Pass','Warning','Fail'))
);
```

Note the deliberate change from the demo: the old `execution_results` table stored **one row per record per rule** — at your scale that is 500K × 150 = 75M rows *per day*. It is replaced by (a) the wide output table, which holds the per-record verdicts, and (b) the small summary tables above for dashboards.

### 2.6 Users, roles, views, audit

```sql
CREATE TABLE app_user (
    user_id      SERIAL PRIMARY KEY,
    username     TEXT NOT NULL UNIQUE,      -- corporate ID (SSO/LDAP subject)
    display_name TEXT,
    email        TEXT,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE role (
    role_id      SERIAL PRIMARY KEY,
    role_name    TEXT NOT NULL UNIQUE       -- Admin, Creator, Approver, Reviewer, Explorer
);

CREATE TABLE user_role (
    user_id      INT NOT NULL REFERENCES app_user(user_id),
    role_id      INT NOT NULL REFERENCES role(role_id),
    workspace    TEXT NOT NULL,             -- role scoped to workspace/LOB
    PRIMARY KEY (user_id, role_id, workspace)
);

CREATE TABLE saved_view (
    view_id      SERIAL PRIMARY KEY,
    view_name    TEXT NOT NULL,
    base_type    TEXT NOT NULL,             -- 'workflow_output' | 'dataset' | 'ref_dataset'
    base_id      INT NOT NULL,              -- workflow_id / dataset_id / ref_dataset_id
    columns_json JSONB NOT NULL,            -- selected columns, in order
    filters_json JSONB NOT NULL DEFAULT '[]', -- [{column, operator, value}] — compiled to parameterized SQL
    row_limit    INT,                       -- null = all rows
    workspace    TEXT NOT NULL,
    created_by   TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace, view_name)
);

CREATE TABLE audit_log (
    audit_id     BIGSERIAL PRIMARY KEY,
    username     TEXT NOT NULL,
    action       TEXT NOT NULL,             -- RULE_APPROVED, WORKFLOW_RUN, OUTPUT_DOWNLOADED, VIEW_CREATED, ...
    entity_type  TEXT NOT NULL,
    entity_id    TEXT,
    details      JSONB,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Role capabilities (matching your five roles):

| Capability | Admin | Creator | Approver | Reviewer | Explorer |
|---|---|---|---|---|---|
| System/user settings | ✔ | | | | |
| Propose rule (English definition) | ✔ | ✔ | ✔ | ✔ | ✔ |
| Review/comment on proposed rule | ✔ | | | ✔ | |
| Approve rule for a dataset | ✔ | | ✔ | | |
| Author/test/publish rule code, build workflows | ✔ | ✔ | | | |
| Run workflow manually | ✔ | ✔ | | | |
| Create views, download CSV | ✔ | ✔ | ✔ | ✔ | ✔ |

Saved-view filters are compiled server-side into parameterized SQL (`WHERE col = %s ...`) — user filter values are never interpolated into SQL strings.

---

## 3. Output Tables (schema `dq_output`) — Wide, Partitioned, Self-Retiring

One physical table per workflow, created by the platform when the workflow is activated:

```sql
CREATE TABLE dq_output.wf_12_acct_master (
    rec_id       BIGINT      NOT NULL,
    batch_date   DATE        NOT NULL,
    run_id       INT         NOT NULL,
    -- original dataset columns, in source order (from dataset_column) --
    acct_no      TEXT,
    fname        TEXT,
    mname        TEXT,
    sname        TEXT,
    city         TEXT,
    -- ... up to ~250 columns ...
    -- result columns, in workflow_rule_mapping.display_order --
    "R101_city"  CHAR(1),        -- 'P' / 'F', rendered as Pass/Fail in UI and CSV export
    "R102_fname" CHAR(1),
    "R102_sname" CHAR(1)
    -- ...
) PARTITION BY RANGE (batch_date);
```

Daily partitions (`..._p20260714` for `2026-07-14`), created by the engine just before writing. This single decision solves three requirements at once:

1. **90-day retention (Q24):** after each successful run, the engine drops partitions older than `dataset.retention_batches` days. `DROP PARTITION` is instantaneous metadata surgery — no multi-minute `DELETE` of 500K rows, no table bloat, no vacuum pressure. Monthly feeds simply use monthly partitions with `retention_batches = 3` (or as required).
2. **Same-day overwrite (Q8):** re-run = truncate that day's partition, re-insert. Idempotent.
3. **Failed-run cleanup (Q28):** a run writes into its partition only after all rules succeed in memory; if anything fails before the write, there is nothing to clean, and if the bulk write itself fails, the partition is truncated. The output table never holds a partial run.

**Adding a rule later (Q23):** the workflow editor issues `ALTER TABLE ... ADD COLUMN "R103_state" CHAR(1)` — instant in PostgreSQL (metadata-only, no rewrite). Historical partitions show NULL for the new column, exactly as you specified. Removing a mapping deactivates it (`is_active = FALSE`) and stops populating the column; the column is kept for history.

**Column-count guardrail:** PostgreSQL allows ~1600 columns per table. 250 original + 3 system + result columns leaves room for ~1300 rule-column mappings per workflow — far beyond the ~150-rule reality, but the workflow editor should still validate the ceiling.

---

## 4. Rule Contract — Standardized, Convertible from Your Existing 150 Rules

Every `rule_version.python_expression` is the **body of a function with a fixed signature**. This is what makes safe dynamic assembly, pre-run validation, and future `fail_reason` support possible without ever restructuring.

```python
def evaluate(df: pd.DataFrame, column: str, lookups: dict, params: dict) -> pd.Series:
    """
    df       : the full input batch (read-only by convention)
    column   : the target column this mapping applies the rule to
    lookups  : {lookup_alias: pd.Series of allowed values} per rule_lookup_binding
    params   : per-mapping parameters (workflow_rule_mapping.parameters merged over defaults)
    returns  : boolean Series aligned to df.index — True = Pass, False = Fail
    """
```

Because `column` is a parameter, one rule definition serves every column it is mapped to — the engine calls it once per mapping. Multi-column rules (like First Name checking `mname`) read the secondary columns directly from `df`, and declare them in `rule_version.required_columns` so the engine can verify they exist *before* the run starts (your Q15 failure mode: missing input → fail fast with a clear message, whole run aborts).

### 4.1 Your sample rules, converted

City Is Valid Check — stored `python_expression` (note it works for any mapped column, and length threshold became a parameter):

```python
s = df[column]
min_len = params.get("min_length", 1)
result = (
    s.notna()
    & (s.str.strip().str.len() > min_len)
    & (
        s.str.match(r"^[a-zA-Z\s]*$", na=False)
        | s.str.contains("PALMS", na=False)
    )
)
return result
```

First Name Is Valid Check — invalid word lists move out of the code and into a lookup table (your existing `invalid_words_name_address` becomes a registered `ref_dataset`), bound as `lookup_alias = 'invalid_words'`. `required_columns = ["mname"]`.

```python
s = df[column].astype(str).str.strip()
s_upper = s.str.upper()
mname = df["mname"].astype(str).str.strip().str.lower()

invalid = (
    s_upper.isin(lookups["invalid_words"])
    | (s == "")
    | s.str.startswith("-")
    | s.str.lower().str.contains("est")
    | (mname == "of")
    | (~s.str.match(r"^[a-zA-Z\-,'\s]*$", na=False))
)
return ~invalid
```

A pure lookup rule (state code must exist in reference table) becomes a two-liner — this pattern likely covers a large share of your 150 rules:

```python
return df[column].astype(str).str.strip().str.upper().isin(
    lookups["valid_values"].astype(str).str.strip().str.upper()
)
```

Migrating from lists hard-coded in Python to lookup tables also means business can maintain the invalid-word lists through the upload wizard without a rule code change and re-approval cycle — worth confirming with your governance team whether lookup-content changes require re-approval.

### 4.2 Future fail_reason (Q17), designed-in now

The contract returns a boolean Series today. When fail reasons are needed, a v2 contract lets a rule optionally return `(bool_series, reason_series)`; the engine already normalizes every rule result through one `RuleResult` wrapper internally, so only the persistence layer gains an optional `"R101_city_reason"` column. No schema or rule rewrites.

---

## 5. Execution Engine — In-Memory Dynamic Assembly

No physical `ruleengine.py` is written (Q27). The engine assembles each approved rule's expression into a function object at run time, executes, and stores the full generated source + execution log in `workflow_run.engine_log` for review.

### 5.1 Run lifecycle

```
Trigger (file watcher or Run button)
  └─ create workflow_run (Queued)          ── unique index blocks duplicate active runs
Dispatcher picks run (capacity available)
  1. VALIDATE   count file vs actual rows; file readable; all mappings resolve to an
                Approved rule_version; all required_columns exist in dataset;
                all lookup bindings resolve to active ref_datasets      → else Fail fast
  2. SNAPSHOT   record rule_versions_used (audit: exactly which code ran)
  3. LOAD       read CSV (dtype=str, keep_default_na=False for fidelity);
                assign rec_id; load lookup Series from dq_ref tables
  4. COMPILE    for each mapping: compile(python_expression) inside the
                evaluate() wrapper in a restricted namespace (pd, np, re only)
  5. EXECUTE    mappings in display_order; each returns boolean Series → 'P'/'F' column
                progress_pct advances per mapping → progress bar
                any exception → status=Failed, error_message set, nothing written (Q28)
  6. WRITE      create/truncate today's partition; single COPY of the full wide
                frame via psycopg2 copy_expert (~500K rows in well under a minute)
  7. SUMMARIZE  insert run_rule_summary + run_group_summary (weighted layer-2 scores)
  8. RETIRE     drop partitions beyond retention_batches
  9. COMPLETE   status=Completed, progress=100
```

Skeleton of the core (illustrative):

```python
ALLOWED_GLOBALS = {"pd": pd, "np": np, "re": re, "__builtins__": SAFE_BUILTINS}

def build_rule_fn(expression: str):
    src = "def evaluate(df, column, lookups, params):\n" + indent(expression, "    ")
    ns = {}
    exec(compile(src, "<rule>", "exec"), dict(ALLOWED_GLOBALS), ns)
    return ns["evaluate"]

for i, m in enumerate(mappings, start=1):
    run.update(current_step=f"Executing rule {i}/{len(mappings)}: {m.rule_name} on {m.column_name}",
               progress_pct=10 + 70 * i / len(mappings))
    fn = build_rule_fn(m.python_expression)
    passed = fn(df, m.column_name, m.lookups, m.params)          # boolean Series
    out[m.result_column] = np.where(passed.fillna(False), "P", "F")
```

The restricted namespace is a guardrail against accidents (a rule importing os, opening files, mutating the database), not a security sandbox — appropriate because only the Creator role authors code and every expression passes UAT before Approved status. `SAFE_BUILTINS` whitelists `len, range, str, int, float, min, max, sorted, enumerate, zip` etc. and excludes `open`, `__import__`, `exec`, `eval`.

### 5.2 Triggering (Q20) and the count file

The worker runs a poller (every 1–2 min) over active datasets' `source_path`. When both the data file and its count file for a new `batch_date` are present: parse expected count, register/refresh `dataset_batch`, verify row count during LOAD (mismatch → batch marked `CountMismatch`, run fails with a precise message), then enqueue runs for every Active workflow with `trigger_mode='auto'` on that dataset. The Run button creates the same queue entry with `trigger_type='manual'` — one code path for both.

### 5.3 Queue and parallelism (Q21, Q30)

Phase 1 keeps it deliberately simple: the dispatcher is a loop inside the worker pod that claims the oldest Queued run using `SELECT ... FOR UPDATE SKIP LOCKED` and executes runs in a `ProcessPoolExecutor(max_workers=N)` — start with N=2 (two parallel workflows comfortably fit in memory; same-workflow overlap is impossible thanks to the unique index). Raising N or scaling to multiple worker pods later requires no schema change, because the DB-backed queue with `SKIP LOCKED` already coordinates multiple consumers correctly. Celery/Redis stays off the dependency list until genuinely needed.

### 5.4 Progress bar (Q29)

`workflow_run.progress_pct` + `current_step` are updated at each phase boundary and after every rule. React polls `GET /runs/{id}/status` every 2 seconds while a run is live (simplest; SSE is a drop-in upgrade later). The user sees "Executing rule 7/23: City Is Valid Check — 42%" exactly like the Data Studio experience.

---

## 6. Security & PII (Q35)

Names/addresses at credit-bureau scale demand layered controls: TLS on all pod-to-pod and client traffic (OpenShift routes with edge/reencrypt termination); PostgreSQL on encrypted storage class (encryption at rest handled by the platform PVC); authentication via corporate SSO/LDAP with the app never storing passwords; RBAC per §2.6 with workspace scoping so an LOB's users see only their datasets; every download of output data recorded in `audit_log` (who, which view/table, row count, when); no PII in application logs or in `engine_log` (log rule names and counts, never data values); and DB credentials in OpenShift Secrets, not config files. Column-level masking for the Explorer role (e.g., masked names in views) is a natural later add because views are already server-compiled — flagging it as a Phase-4 option rather than a day-one requirement.

---

## 7. Migration & Parallel Validation (Q36–37)

You already hold the two hardest assets: 150 exported rules already converted to Python and tested. Migration is then mechanical: wrap each tested snippet into the §4 contract (mostly renaming the hard-coded column to `column` and lists to lookups), load via a one-time script into `rule`/`rule_version` with status ApprovedUAT, and re-verify in UAT.

For cutover confidence, build a small **comparison utility** into the platform (Creator/Admin only): upload the Experian Data Studio output export for the same batch, join to the new output on the natural key, and report per rule-column: match count, mismatch count, and a sample of mismatching rec_ids with both verdicts. Run both systems in parallel for an agreed window (e.g., 2–4 weeks of daily batches with zero unexplained mismatches) before decommissioning Data Studio. Mismatch reports themselves become UAT evidence for sign-off.

---

## 8. Phased Delivery Plan

**Phase 1 — Foundation & engine core (the critical path).** PostgreSQL on OpenShift; `dq_meta` schema migration from SQLite; ref-dataset wizard rewired to register in `ref_dataset`; rule/rule_version loading script for the 150 converted rules; execution engine end-to-end for one real workflow with manual trigger, partitioned output, summaries, and retention. Exit: one production dataset processed daily by manual run, output matching Data Studio.

**Phase 2 — Automation & operations.** NDM file watcher + count-file validation; DB-backed queue with 2-way parallelism; progress bar; failure notifications; same-day overwrite re-run flow.

**Phase 3 — Governance UI.** Rule lifecycle wizard (propose → review → approve → publish) with role enforcement and notifications; workflow designer with the Data Studio-style column pick lists, mapping order, parameters, and rule groups with weights; views module (dataset/columns/rows/filter/save/download) against output tables.

**Phase 4 — Scale-out & hardening.** Remaining workflows onboarded LOB by LOB; parallel-validation utility and cutover sign-offs; charting module integration; optional Explorer-role masking; optional Impala direct-connect source type (schema already anticipates it via `dataset.source_type`).

---

## 9. Open Points — Resolutions

1. **Lookup content changes — RESOLVED:** business-maintainable, no rule re-approval cycle. Business shares the updated list in Excel; an Admin uploads it through the dataset-page wizard (name, columns, data types), which overwrites the existing lookup table or creates a new one. Rules pick up new values on the next run automatically. (openpyxl/xlrd cover the Excel intake.)
2. **Group scoring — RESOLVED:** per-workflow threshold bands on the (weighted) pass rate, e.g., Pass ≥ 99.0%, Fail < 85.0%, Warning between. Modeled as `pass_threshold`/`fail_threshold` on `workflow_rule_group`; `run_group_summary.group_status` stores the derived verdict. (§2.4)
3. **Auth — RESOLVED:** authentication via AD group / SSO (ldap3 + PyJWT); authorization validated locally in the app against the `user_role` table — exactly the LDAP-for-identity, app-for-roles split in §1.2.
4. **Comparison join key — RESOLVED:** account number is not guaranteed, so the join key is file row position, which both systems share because they process the identical file. Our `rec_id = batch_id × 10⁹ + row_number` is that positional key as a BIGINT (capacity far beyond 45M rows). The comparison utility joins on (batch_date, row number), reconstructing row numbers from the order-preserving Experian export.
5. **Write strategy — DECIDED (single write):** raw input is not pre-loaded into the output table and later overwritten; the engine computes all results in memory and writes the finished wide rows into the day's partition once. This avoids double-writing 500K wide rows, avoids a visible half-processed state, and keeps failure cleanup trivial. If pre-processing raw-data visibility is ever required, a per-dataset staging table (truncated each batch) is the designated mechanism — not the output table. Monthly-feed retention defaults to 3 batches via `dataset.retention_batches`, adjustable per dataset.
