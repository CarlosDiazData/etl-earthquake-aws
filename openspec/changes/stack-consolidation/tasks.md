# Tasks: Stack Consolidation (5→3)

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~1,300 (630 add + 670 del) |
| 400-line budget risk | High |
| Chained PRs recommended | Yes |
| Suggested split | PR 1: Create PipelineStack → PR 2: Wire + test + cleanup |
| Delivery strategy | ask-on-risk |
| Chain strategy | pending |

Decision needed before apply: Yes
Chained PRs recommended: Yes
Chain strategy: pending
400-line budget risk: High

### Suggested Work Units

| Unit | Goal | Likely PR | Notes |
|------|------|-----------|-------|
| 1 | Create PipelineStack with all 4 sections | PR 1 | base: feature/stack-consolidation; additive only, ~520 new lines |
| 2 | Wire app.py, update tests, delete 3 old stacks | PR 2 | base: PR #1 branch; ~100 add + ~700 del |

## Phase 1: Create PipelineStack

- [x] 1.1 Create `etl_cdk/stacks/pipeline_stack.py` — constructor with `data_bucket`, `scripts_bucket`, `ingestion_lambda`, `app_name`, `env_name` props
- [x] 1.2 Port Glue section: `BucketDeployment`, `CfnDatabase`, IAM role + policies, 2x `CfnJob` (Spark 5.0, 5 G.2X, Delta Lake, bookmarks, X-Ray)
- [x] 1.3 Port Step Functions section: `LogGroup`, `LambdaInvoke` with `result_selector` + `add_catch` (States.ALL), 2x `GlueStartJobRun`, `Choice` (recordsCount), `StateMachine` (4h timeout, X-Ray, ALL logs)
- [x] 1.4 Port Monitoring section: `CfnParameter("AlertEmail")` with email regex, SNS Topic + subscription, 4 alarms, Dashboard
- [x] 1.5 Port EventBridge section: `Rule` with cron `0/6h` → State Machine target

## Phase 2: Wire app.py

- [x] 2.1 Update imports: remove `GlueStack`, `OrchestrationStack`, `MonitoringStack`; add `PipelineStack`
- [x] 2.2 Replace 3 stack instantiations with `PipelineStack(..., data_bucket=, scripts_bucket=, ingestion_lambda=)` + `add_dependency(data_lake)` + `add_dependency(ingestion)`

## Phase 3: Update Tests

- [x] 3.1 Remove old test imports and test functions: `test_glue_stack_*`, `test_orchestration_stack_*`, `test_monitoring_stack_*` (10 functions)
- [x] 3.2 Add `test_pipeline_stack_creates_resources` — 2 Glue jobs, 1 State Machine, 4 alarms, 1 SNS topic, 1 Dashboard, 1 AlertEmail param
- [x] 3.3 Add `test_pipeline_stack_state_machine_has_choice` — Choice state in definition
- [x] 3.4 Add `test_pipeline_stack_has_result_selector` — `result_selector` in LambdaInvoke
- [x] 3.5 Add `test_pipeline_stack_has_add_catch` — `add_catch` on LambdaInvoke
- [x] 3.6 Add `test_pipeline_stack_alert_email_has_validation` — `AllowedPattern` regex
- [x] 3.7 Add `test_pipeline_stack_jobs_have_xray` — `--enable-xray-tracing` in Glue args
- [x] 3.8 Add `test_pipeline_stack_passes_cdk_nag` — AwsSolutionsChecks on PipelineStack

## Phase 4: Cleanup

- [x] 4.1 Delete `etl_cdk/stacks/glue_stack.py`
- [x] 4.2 Delete `etl_cdk/stacks/orchestration_stack.py`
- [x] 4.3 Delete `etl_cdk/stacks/monitoring_stack.py`

## Phase 5: Verification

- [x] 5.1 Run `cdk synth` — verify 3 stacks in output (not 5)
- [x] 5.2 Run `pytest tests/` — all tests pass
- [x] 5.3 Verify PipelineStack template has all expected resources
