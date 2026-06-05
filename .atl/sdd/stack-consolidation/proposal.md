# Proposal: Stack Consolidation (5→3)

## Status

proposed

## Executive Summary

Merge GlueStack, OrchestrationStack, and MonitoringStack into a single PipelineStack. The three stacks are one logical pipeline — separating them adds cross-stack dependency complexity and introduces a CloudFormation ordering bug (MonitoringStack has no `add_dependency`). DataLakeStack and IngestionStack remain unchanged.

## Intent

Consolidate 5 CDK stacks into 3 by merging the ETL pipeline layer (Glue + Orchestration + Monitoring) into one stack. The MonitoringStack currently lacks `add_dependency` on OrchestrationStack/GlueStack — CloudFormation can provision alarms before Glue jobs exist, causing deployment failures. Merging makes dependencies implicit via shared template. Also reduces cognitive overhead: 3 files to reason about instead of 5.

## Scope

### In Scope
- New `etl_cdk/stacks/pipeline_stack.py` merging GlueStack (204 lines), OrchestrationStack (137 lines), MonitoringStack (195 lines)
- Delete `glue_stack.py`, `orchestration_stack.py`, `monitoring_stack.py`
- Update `app.py` to instantiate 3 stacks: DataLakeStack, IngestionStack, PipelineStack
- PipelineStack accepts props: `data_bucket`, `scripts_bucket`, `ingestion_lambda`
- Update `tests/unit/test_etl_stack.py`: remove individual Glue/Orch/Monitoring test functions, add PipelineStack tests covering same assertions (Glue jobs, State Machine, 4 alarms, SNS topic, dashboard)
- Preserve `AlertEmail` CfnParameter with email validation

### Out of Scope
- No ETL logic changes (Lambda code, Glue scripts untouched)
- No new features or monitoring additions
- No schema changes to S3 bucket structure

## Capabilities

### New Capabilities

None — pure refactor, no new spec-level behavior.

### Modified Capabilities

None — all existing requirements (step-functions-choice, monitoring-alerts, xray-tracing, data-bucket-encryption, glue-bookmarks) are preserved unchanged. Resources move between files; CloudFormation template semantics are identical.

## Approach

1. Create `pipeline_stack.py` with three logical sections: Glue resources (jobs, database, IAM role, script deployment), Orchestration resources (Step Functions, EventBridge rule), and Monitoring resources (alarms, SNS topic, dashboard) — all inline, implicit ordering via CDK construct tree
2. PipelineStack constructor accepts `data_bucket`, `scripts_bucket`, `ingestion_lambda` as props (replacing cross-stack references)
3. Update `app.py`: instantiate PipelineStack after IngestionStack, add `add_dependency(ingestion)` and `add_dependency(data_lake)`
4. Update tests: consolidate Glue/Orch/Monitoring tests into PipelineStack tests; preserve all existing assertions
5. Delete old stack files and their `__init__.py` exports

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `etl_cdk/stacks/pipeline_stack.py` | New | Merged Glue + Orchestration + Monitoring |
| `etl_cdk/stacks/glue_stack.py` | Removed | Merged into pipeline_stack.py |
| `etl_cdk/stacks/orchestration_stack.py` | Removed | Merged into pipeline_stack.py |
| `etl_cdk/stacks/monitoring_stack.py` | Removed | Merged into pipeline_stack.py |
| `etl_cdk/stacks/__init__.py` | Modified | Remove 3 exports, add PipelineStack export |
| `app.py` | Modified | 3 stacks instead of 5 |
| `tests/unit/test_etl_stack.py` | Modified | Consolidate into PipelineStack tests |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| CloudFormation logical ID changes trigger resource replacement on first deploy | Medium | Deploy to dev first; accept one-time replacement of alarms/dashboard/topic (no data loss) |
| Old stacks must be torn down before PipelineStack deploy | Medium | Manual stack deletion via `cdk destroy` before `cdk deploy`; document in design |
| Test coverage regression during test consolidation | Low | Run `pytest --cov` before and after; preserve all assertion counts |

## Rollback Plan

1. `git revert` the merge commit to restore 5-stack app.py and all three stack files
2. Deploy old stacks via `cdk deploy --all`
3. If PipelineStack was already deployed, `cdk destroy` it first, then deploy old stacks
4. No data loss risk — S3 buckets, Lambda, and Glue jobs are in DataLakeStack and IngestionStack (unchanged)

## Dependencies

- None — self-contained refactor within existing codebase

## Success Criteria

- [ ] `cdk synth` produces valid templates for all 3 stacks
- [ ] `pytest tests/` passes with same coverage as before
- [ ] PipelineStack template contains: 2 Glue jobs, 1 Glue database, 1 State Machine, 2 EventBridge rules, 4 CloudWatch alarms, 1 SNS topic, 1 CloudWatch Dashboard, 1 AlertEmail parameter
- [ ] `AlertEmail` parameter has email regex validation
- [ ] `cdk-nag` passes on PipelineStack with no errors
