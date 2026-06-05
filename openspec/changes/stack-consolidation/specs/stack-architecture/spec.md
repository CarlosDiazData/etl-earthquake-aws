# Delta for stack-architecture

## ADDED Requirements

### Requirement: PipelineStack consolidation

The system MUST deploy a single PipelineStack replacing GlueStack, OrchestrationStack, and MonitoringStack.

The system MUST accept `data_bucket` (IBucket), `scripts_bucket` (IBucket), and `ingestion_lambda` (IFunction) as constructor props — plus `app_name` and `env_name`.

The PipelineStack MUST contain all resources from the 3 replaced stacks:
- 2 Glue jobs (bronze→silver, silver→gold) with identical configuration (Spark 5.0, 5 G.2X workers, Delta Lake, job bookmarks, X-Ray, metrics)
- Glue Database `gold_earhquakes`
- Step Functions State Machine with LambdaInvoke → Choice state (recordsCount check) → Glue jobs or skip
- EventBridge Rule (cron: every 6 hours)
- 4 CloudWatch alarms (Glue job failures ×2, State Machine failure, pipeline duration >3h)
- SNS topic with `AlertEmail` CfnParameter and email regex validation
- CloudWatch Dashboard

The system MUST deploy Glue scripts from `scripts/` to `scripts_bucket` as part of PipelineStack synthesis.

### Requirement: Migration path

The system SHOULD support greenfield deployment of PipelineStack without prior stack existence.

Resource naming (bucket names, job names, state machine name, etc.) MUST be identical to pre-consolidation naming to prevent data loss.

## MODIFIED Requirements

### Requirement: Stack composition

The system MUST define exactly 3 stacks in `app.py`: DataLakeStack, IngestionStack, PipelineStack (in dependency order).

(Previously: 5 stacks — DataLakeStack, IngestionStack, GlueStack, OrchestrationStack, MonitoringStack)

#### Scenario: 3-stack application

- GIVEN the CDK app entry point (`app.py`)
- WHEN the application synthesizes
- THEN the app produces CloudFormation templates for exactly 3 stacks

#### Scenario: Dependency ordering

- GIVEN PipelineStack depends on DataLakeStack and IngestionStack
- WHEN CloudFormation deploys
- THEN DataLakeStack and IngestionStack provision before PipelineStack

### Requirement: Test coverage

The test suite MUST preserve all existing structural assertions from the 3 removed stacks in consolidated form.

(Previously: tests for each stack individually — now consolidated under PipelineStack)

#### Scenario: Consolidated assertions

- GIVEN a synthesized PipelineStack template
- WHEN assertions run
- THEN the template contains 2 Glue jobs, 1 Glue database, 1 State Machine, 1 EventBridge rule, 4 CloudWatch alarms, 1 SNS topic, 1 CloudWatch Dashboard, 1 AlertEmail parameter
- AND the State Machine contains the Choice state with recordsCount check
- AND the State Machine contains the `result_selector` and `add_catch` for Lambda failure handling

## REMOVED Requirements

### Requirement: Individual stack separation

GlueStack, OrchestrationStack, and MonitoringStack MUST be removed from the project.

(Reason: resources merged into PipelineStack — separate stacks added cross-stack complexity and caused a CloudFormation ordering bug)
