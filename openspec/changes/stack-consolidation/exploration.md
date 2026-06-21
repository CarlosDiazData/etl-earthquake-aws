## Exploration: Stack Consolidation (5→3) — Develop Branch Delta Analysis

### Background

This exploration analyzes how the **develop** branch changes (28-line delta across IngestionStack and OrchestrationStack) affect the stack consolidation plan that merges GlueStack + OrchestrationStack + MonitoringStack into a single PipelineStack.

Prior work exists: `.atl/sdd/stacks-exploration/explore.md` (detailed per-stack analysis) and `.atl/sdd/stack-consolidation/proposal.md` (proposal for the consolidation). This exploration updates the proposal with develop-branch specifics.

---

### Current State (develop branch)

**5 stacks**, wired in `app.py` with explicit `add_dependency`:

```
DataLakeStack ──→ IngestionStack ──→ OrchestrationStack ──→ MonitoringStack
      │                  │                  │                     │
      └──→ GlueStack ───┘                  │                     │
                                           └─────────────────────┘
```

The develop branch adds 28 lines of delta across two stacks:

| Stack | Change | Lines | Purpose |
|-------|--------|-------|---------|
| `ingestion_stack.py` | KMS permissions (`GenerateDataKey`, `Decrypt`) on Lambda role | +11 | Lambda needs KMS permissions to write/read encrypted S3 objects |
| `orchestration_stack.py` | `result_selector` to extract `$.Payload.recordsCount` | +4 | Extracts the records count from Lambda response payload |
| `orchestration_stack.py` | `add_catch` with `States.ALL` → `LambdaFailed` Pass state | +13 | Catches Lambda failures, sets `recordsCount=0` so Choice has a valid path |

---

### Develop Changes — Impact on Consolidation

#### 1. IngestionStack KMS Permissions → **NO IMPACT**

IngestionStack stays as an independent stack. The KMS permission block (lines 63-73) is a local policy addition that does not change IngestionStack's contract with other stacks.

**Verdict**: Include in the consolidated plan as unchanged. The KMS block remains in `ingestion_stack.py`, which is not touched by the merge.

#### 2. OrchestrationStack `result_selector` and `add_catch` → **MUST PORT TO PIPELINESTACK**

These are in OrchestrationStack, which gets absorbed into PipelineStack. Both must be ported verbatim:

```python
# result_selector — extracts recordsCount from Lambda response
invoke_lambda = tasks.LambdaInvoke(
    ...,
    result_selector={
        "recordsCount.$": "$.Payload.recordsCount",
    },
)

# add_catch — handles Lambda failures gracefully
invoke_lambda.add_catch(
    handler=sfn.Pass(
        self,
        "LambdaFailed",
        parameters={
            "recordsCount": 0,
            "error": "lambdaError",
        },
    ),
    errors=["States.ALL"],
)
```

**Note**: These are already present in the working tree (develop). If you generate PipelineStack from the current develop files, they are included automatically. No extra step needed.

---

### Cross-Stack Dependency Map (Current)

| Exported Attribute | From Stack | Used By | Stays/Goes |
|-------------------|------------|---------|------------|
| `data_bucket` | DataLakeStack | Ingestion, Glue, Orchestration, Monitoring | **Stays** (all exported) |
| `scripts_bucket` | DataLakeStack | Glue | **Stays** (exported to PipelineStack) |
| `ingestion_lambda` | IngestionStack | Orchestration | **Stays** (exported to PipelineStack) |
| `bronze_to_silver_job` | GlueStack | Orchestration, Monitoring | **Becomes internal** (PipelineStack creates both jobs internally) |
| `silver_to_gold_job` | GlueStack | Orchestration, Monitoring | **Becomes internal** |
| `state_machine` | OrchestrationStack | Monitoring | **Becomes internal** |

**Key insight**: After merge, the Orchestration→Glue and Monitoring→Glue/Orchestration cross-stack references all become internal references within PipelineStack. This eliminates 3 constructor parameters and implicit dependency ordering issues.

---

### PipelineStack Props Proposal

When merging, the new `PipelineStack` constructor needs exactly these external props:

```python
class PipelineStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        app_name: str,
        env_name: str,
        data_bucket: s3.Bucket,       # from DataLakeStack
        scripts_bucket: s3.Bucket,    # from DataLakeStack
        ingestion_lambda: _lambda.Function,  # from IngestionStack
        **kwargs,
    ) -> None:
```

**Rationale**:
- `data_bucket`: needed by Glue jobs (S3 access, Spark temp dir, Delta checkpoints), Step Functions (Glue job args), Monitoring (S3 dashboard widget)
- `scripts_bucket`: needed by Glue script deployment (BucketDeployment) and job script locations
- `ingestion_lambda`: needed by Step Functions LambdaInvoke task

**Not needed as props** (will be internal):
- `bronze_to_silver_job` — created inside PipelineStack
- `silver_to_gold_job` — created inside PipelineStack
- `state_machine` — created inside PipelineStack

---

### app.py Wiring After Consolidation

```python
data_lake = DataLakeStack(app, ...)

ingestion = IngestionStack(app, ..., data_bucket=data_lake.data_bucket)
ingestion.add_dependency(data_lake)

pipeline = PipelineStack(
    app, ...,
    data_bucket=data_lake.data_bucket,
    scripts_bucket=data_lake.scripts_bucket,
    ingestion_lambda=ingestion.ingestion_lambda,
)
pipeline.add_dependency(ingestion)
pipeline.add_dependency(data_lake)
```

No `MonitoringStack(...)` call — all monitoring resources are inside PipelineStack.

---

### Test Impact Analysis

**File**: `tests/unit/test_etl_stack.py`

#### Tests that remain unchanged (DataLake + Ingestion):

| Test | Line | Notes |
|------|------|-------|
| `test_data_lake_stack_creates_buckets` | 19 | Unchanged |
| `test_data_lake_stack_has_lifecycle_rules` | 42 | Unchanged |
| `test_ingestion_stack_creates_lambda` | 65 | Unchanged |
| `test_ingestion_stack_creates_eventbridge_rule` | 91 | Unchanged |
| `test_data_lake_stack_uses_kms_encryption` | 304 | Unchanged |
| `test_ingestion_stack_lambda_has_xray_tracing` | 318 | Unchanged |
| `test_data_lake_stack_passes_cdk_nag` | 240 | Unchanged |
| `test_ingestion_stack_passes_cdk_nag` | 364 | Unchanged |

#### Tests that need consolidation into PipelineStack equivalents:

| Test | Line | Replace With |
|------|------|-------------|
| `test_glue_stack_creates_jobs` | 106 | PipelineStack: assert 2 Glue jobs exist |
| `test_glue_stack_creates_database` | 133 | PipelineStack: assert GoldDatabase exists |
| `test_orchestration_stack_creates_state_machine` | 155 | PipelineStack: assert 1 State Machine exists |
| `test_monitoring_stack_creates_alarms` | 195 | PipelineStack: assert 4 alarms, 1 SNS topic, 1 dashboard |
| `test_orchestration_stack_has_choice_state` | 266 | PipelineStack: assert Choice state in definition |
| `test_glue_stack_jobs_have_xray_tracing` | 341 | PipelineStack: assert `--enable-xray-tracing` on Glue jobs |
| `test_glue_stack_passes_cdk_nag` | 390 | PipelineStack cdk-nag check |
| `test_orchestration_stack_passes_cdk_nag` | 417 | PipelineStack cdk-nag check |
| `test_monitoring_stack_passes_cdk_nag` | 461 | PipelineStack cdk-nag check |
| `test_monitoring_stack_alert_email_has_validation` | 515 | PipelineStack: assert AlertEmail CfnParameter with pattern |

**Total tests to consolidate**: 10 individual tests → roughly 4-5 integrated PipelineStack tests (creates resources, state machine contract, monitoring contract, email validation, cdk-nag).

#### New tests needed for develop-specific changes:

| New Test | Purpose |
|----------|---------|
| `test_pipeline_stack_has_result_selector` | Verify State Machine definition extracts `$.Payload.recordsCount` |
| `test_pipeline_stack_has_lambda_catch` | Verify State Machine has LambdaFailed catch handler with `recordsCount=0` |

---

### Risks and Gotchas

1. **CloudFormation logical ID changes**: Merging stacks changes all logical IDs (construct paths are different). This triggers resource replacement on first deploy for alarms, SNS topic, dashboard, and log group — but these are stateless resources with no data loss.

2. **Stack teardown ordering**: Old stacks (Glue, Orchestration, Monitoring) must be destroyed before deploying PipelineStack (can't have same resource in two stacks). Since Glue jobs and the State Machine reference S3 buckets and Lambda (which are in other stacks), deletion order matters.

3. **`prosses_silver_gold.py` typo**: The script file is named `prosses_silver_gold.py` (line 140 of glue_stack.py). If renamed during consolidation, the Glue job's `script_location` must match. Either keep the typo or fix it as a separate change.

4. **BucketDeployment timing**: The `BucketDeployment` (s3_deployment) in GlueStack deploys scripts at deploy time. In PipelineStack, this construct will still reference `scripts_bucket` from DataLakeStack — same as before, no issue.

5. **MonitoringStack `add_dependency` bug fixed implicitly**: The existing MonitoringStack has no explicit `add_dependency`, meaning CloudFormation could try to create alarms referencing non-existent Glue jobs. Merging into PipelineStack fixes this because CDK construct tree ordering ensures Glue jobs are created before alarms that reference them.

6. **Develop branch changes are additive only**: Both the KMS permissions and the `result_selector`/`add_catch` are additive changes. They do not conflict with the consolidation plan in any way.

---

### Ready for Proposal

**Yes** — the existing proposal (`openspec/changes/stack-consolidation/proposal.md`) is already sound. The develop branch changes are additive and do not alter the consolidation plan:

- KMS permissions are in IngestionStack (untouched by merge)
- `result_selector` and `add_catch` are in OrchestrationStack (merged into PipelineStack as-is)

**Next recommended phase**: `sdd-design` (move from high-level proposal to detailed technical design, including the merged `pipeline_stack.py` structure and the test consolidation plan).

---

**Status**: success
**Summary**: Explored how the 28-line develop branch delta (KMS permissions in IngestionStack, result_selector + add_catch in OrchestrationStack) integrates with the stack consolidation plan. No conflicts identified — KMS changes stay in untouched IngestionStack; result_selector/add_catch port verbatim into PipelineStack.
**Artifacts**: `openspec/changes/stack-consolidation/exploration.md`
**Next**: sdd-design (proposal exists, design is next logical step)
**Risks**: Logical ID changes on first deploy, stack teardown ordering required, `prosses_silver_gold.py` typo preserved or fixed
**Skill Resolution**: paths-injected
