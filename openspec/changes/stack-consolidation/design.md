# Design: Stack Consolidation (5→3)

## Technical Approach

Merge GlueStack (204 lines), OrchestrationStack (153 lines), and MonitoringStack (195 lines) into a single `PipelineStack` class. No resource changes — pure relocation. Cross-stack references (glue jobs → orchestration, state machine → monitoring) become internal construct-tree references, eliminating the MonitoringStack ordering bug. Develop-branch features (`result_selector`, `add_catch`) port verbatim.

## Architecture Decisions

| # | Decision | Choice | Alternatives | Rationale |
|---|----------|--------|--------------|-----------|
| 1 | Stack granularity | Single PipelineStack | Keep 3 stacks, 2 stacks (Glue+Orch, Monitoring separate) | 3 stacks were one logical pipeline. Merging eliminates cross-stack dependency complexity and the MonitoringStack `add_dependency` bug. Single stack is simplest. |
| 2 | Resource naming | Preserve exact `name=` values | Generate new names, use CDK auto-naming | Prevents resource replacement on first deploy. SNS topic, alarms, dashboard are stateless but keeping names avoids confusion. |
| 3 | Props interface | `data_bucket`, `scripts_bucket`, `ingestion_lambda` as IBucket/IFunction | Pass raw ARNs as strings | L3/L2 construct props preserve type safety and allow CDK to infer dependencies. |
| 4 | Internal organization | 4 logical sections with header comments | Single flat constructor | ~500-line constructor needs section markers for readability. Group by resource domain, not by original stack. |
| 5 | Develop-branch delta | Port `result_selector` and `add_catch` from develop's OrchestrationStack | Revert to simpler version without catch | The `add_catch` prevents State Machine failure when Lambda errors — essential for production resilience. |

## Data Flow

```
EventBridge (cron 0/6h) → StateMachine → LambdaInvoke(result_selector)
                                              │
                                    ChoiceState(recordsCount)
                                    ┌────────┼────────┐
                                  =0       >0        else
                                   │        │          │
                                Pass   GlueJob   Pass("Unexpected")
                              "NoRecs" Bronze→Silver
                                         │
                                      GlueJob
                                    Silver→Gold
```

## File Changes

| File | Action | Description |
|------|--------|-------------|
| `etl_cdk/stacks/pipeline_stack.py` | **Create** | Merged stack — 4 sections: Glue, Step Functions, Monitoring, EventBridge |
| `etl_cdk/stacks/glue_stack.py` | **Delete** | Absorbed into pipeline_stack.py |
| `etl_cdk/stacks/orchestration_stack.py` | **Delete** | Absorbed into pipeline_stack.py |
| `etl_cdk/stacks/monitoring_stack.py` | **Delete** | Absorbed into pipeline_stack.py |
| `app.py` | **Modify** | 3 imports removed (Glue, Orch, Monitoring), 1 import added (PipelineStack). Instantiation after IngestionStack with `add_dependency(ingestion)` |
| `tests/unit/test_etl_stack.py` | **Modify** | Delete 10 individual-stack tests, add 7 PipelineStack tests |

## Constructor Interface

```python
class PipelineStack(Stack):
    def __init__(
        self, scope, construct_id, *,
        app_name: str, env_name: str,
        data_bucket: IBucket,          # from DataLakeStack
        scripts_bucket: IBucket,       # from DataLakeStack
        ingestion_lambda: IFunction,   # from IngestionStack
        **kwargs,
    ) -> None:
```

Internal attributes exposed for testing: `bronze_to_silver_job`, `silver_to_gold_job`, `state_machine` — same names as before so test assertions don't change.

## Internal Organization (Section Map)

PipelineStack constructor body groups resources into **4 logical sections** with header comments:

1. **Section: Glue Resources** — `BucketDeployment` (scripts → S3), `CfnDatabase` (gold_earthquakes), IAM Role + policies, two `CfnJob` instances. Jobs retain Spark 5.0, 5 G.2X workers, Delta Lake extensions, bookmarks, X-Ray. Script locations: `s3://{scripts_bucket}/scripts/process_bronze_to_silver.py` and `process_silver_to_gold.py`.

2. **Section: Step Functions Resources** — LogGroup, `LambdaInvoke` task with `result_selector={"recordsCount.$": "$.Payload.recordsCount"}` and `add_catch` (States.ALL → Pass with recordsCount=0). Two `GlueStartJobRun` tasks. `Choice` state on `$.recordsCount` (0 → skip, >0 → run pipeline, else → UnexpectedState). `StateMachine` with 4h timeout, X-Ray, ALL logs.

3. **Section: Monitoring Resources** — SNS Topic. `CfnParameter("AlertEmail")` with email regex, default `admin@example.com`. Email subscription. 4 CloudWatch Alarms: BronzeToSilverFailed, SilverToGoldFailed, StateMachineFailed, PipelineDurationExceeded (>3h). Dashboard: Glue executions, SFN executions, pipeline duration, S3 data volume.

4. **Section: EventBridge Schedule** — `Rule` with cron `minute=0, hour=0/6`, target: State Machine.

## app.py Wiring

```python
pipeline = PipelineStack(app, f"{APP_NAME}-pipeline-{ENV_NAME}",
    app_name=APP_NAME, env_name=ENV_NAME,
    data_bucket=data_lake.data_bucket,
    scripts_bucket=data_lake.scripts_bucket,
    ingestion_lambda=ingestion.ingestion_lambda,
    env=deploy_env,
)
pipeline.add_dependency(data_lake)
pipeline.add_dependency(ingestion)
```

Stack order: DataLakeStack → IngestionStack → PipelineStack. 3 stacks total.

## Testing Strategy

| Layer | What | Approach |
|-------|------|----------|
| Unit | Resource count | 2 Glue jobs, 1 State Machine, 4 alarms, 1 SNS topic, 1 dashboard, 1 AlertEmail param |
| Unit | State Machine contract | Choice state in definition, result_selector present, add_catch present |
| Unit | Email validation | AlertEmail CfnParameter has AllowedPattern |
| Unit | X-Ray tracing | Glue job default arguments include `--enable-xray-tracing` |
| Integration | cdk-nag | Single PipelineStack AwsSolutionsChecks pass (replaces 3 individual checks) |

**7 consolidated tests**: `test_pipeline_stack_creates_resources`, `test_pipeline_stack_state_machine_has_choice`, `test_pipeline_stack_has_result_selector`, `test_pipeline_stack_has_add_catch`, `test_pipeline_stack_alert_email_has_validation`, `test_pipeline_stack_jobs_have_xray`, `test_pipeline_stack_passes_cdk_nag`.

## Migration Procedure

Resource names are preserved (`name=` parameter unchanged), but **construct paths change** (from `GlueStack/BronzeToSilverJob` to `PipelineStack/BronzeToSilverJob`). CloudFormation treats these as new resources. Must destroy old stacks first:

1. `cdk destroy MonitoringStack` — stateless resources (alarms, dashboard, SNS topic)
2. `cdk destroy OrchestrationStack` — State Machine, EventBridge rule, LogGroup
3. `cdk destroy GlueStack` — Glue jobs, database, IAM role, BucketDeployment
4. `cdk deploy PipelineStack` — greenfield deployment of all merged resources

No data loss: S3 buckets and Lambda are in DataLakeStack and IngestionStack (untouched).

## Open Questions

- None. All resources and develop-branch features are accounted for.
