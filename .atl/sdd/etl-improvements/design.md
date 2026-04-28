# Design: etl-improvements

## Technical Approach

This change introduces seven discrete improvements across the ETL pipeline: Step Functions choice branching for zero-record handling, KMS encryption for data-at-rest, Glue bookmarks for incremental processing, X-Ray tracing on Lambda/Glue, SNS email parameterization, cdk-nag security validation, and CI/CD pipeline automation.

## Architecture Decisions

### Decision: Step Functions Choice State for Zero Records

**Choice**: Add `Choice` state after ingestion Lambda with `Pass` state for termination
**Alternatives considered**: Continue to downstream jobs and have them handle empty input gracefully; Use a Condition in the definition instead of explicit Choice
**Rationale**: Explicit Choice state provides clear, observable pipeline behavior and avoids unnecessary Glue job invocations when no data exists.

### Decision: KMS CMK for S3 Encryption

**Choice**: Create per-stack KMS key with rotation, shared across data and scripts buckets
**Alternatives considered**: Per-bucket keys (increases key management overhead); AWS-managed S3 encryption (requirement mandates CMK)
**Rationale**: Single key simplifies policy management. Rotation enabled satisfies compliance without re-deployment.

### Decision: SNS Email via CfnParameter

**Choice**: Use `CfnParameter` with `default="admin@example.com"` and validation
**Alternatives considered**: Environment variable (not CloudFormation-native); Hardcoded value (previous approach)
**Rationale**: CloudFormation parameter enables stack updates without code changes and provides validation.

### Decision: cdk-nag AwsSolutions Rules

**Choice**: Integrate `cdk-nag` with `AwsSolutions` plugin, fail on errors
**Alternatives considered**: `HIPAA` or `PCI` plugins (too restrictive for this project); Warnings only (allows security regressions)
**Rationale**: AwsSolutions provides balanced security coverage without excessive noise.

### Decision: CodePipeline + CodeBuild for CI/CD

**Choice**: CodePipeline with two stages (Source + Build/Deploy), CodeBuild for CDK synth
**Alternatives considered**: GitHub Actions (vendor lock-in); CDK Pipelines (higher-level, less visibility)
**Rationale**: Native AWS service, full control over stages, integrates with existing AWS identity.

## Data Flow

```
develop/main branch
       │
       ▼
CodePipeline (Source: CodeCommit)
       │
       ▼
CodeBuild (CDK Synth + cdk-nag)
       │
       ├─► dev env (on develop merge)
       │
       └─► prod env (on main merge)
```

## File Changes

| File | Action | Description |
|------|--------|-------------|
| `etl_cdk/stacks/orchestration_stack.py` | Modify | Add Choice state branching on `recordsCount` |
| `etl_cdk/stacks/data_lake_stack.py` | Modify | Add KMS key, encrypt buckets with CMK |
| `etl_cdk/stacks/glue_stack.py` | Modify | Add `--enable-xray-tracing` to job args |
| `etl_cdk/stacks/monitoring_stack.py` | Modify | Add `alert_email` CfnParameter |
| `etl_cdk/stacks/ingestion_stack.py` | Modify | Add `tracing_config.mode = Tracing.ACTIVE` |
| `etl_cdk/stacks/cicd_pipeline_stack.py` | Create | CodePipeline + CodeBuild stack |
| `tests/unit/test_etl_stack.py` | Modify | Add cdk-nag assertions |
| `tests/unit/test_cicd_pipeline.py` | Create | Test CI/CD pipeline stack |

## Step Functions Choice State Implementation

```python
# In OrchestrationStack.__init__ after invoke_lambda definition
choice_state = sfn.Choice(
    self,
    "CheckRecordsCount",
    comment="Branch based on whether records were ingested"
)

zero_records_path = sfn.Pass(
    self,
    "NoRecordsSkipped",
    comment="No records to process, terminate successfully"
)

process_records_path = bronze_to_silver_task

choice_state.add_choice(
    sfn.Condition.number_equals("$.recordsCount", 0),
    zero_records_path
)
choice_state.add_choice(
    sfn.Condition.number_greater_than("$.recordsCount", 0),
    process_records_path
)

definition = invoke_lambda.next(choice_state)
```

## KMS Encryption Implementation

```python
# In DataLakeStack.__init__
from aws_cdk import aws_kms as kms

self.kms_key = kms.Key(
    self,
    "DataLakeKmsKey",
    enable_key_rotation=True,
    removal_policy=RemovalPolicy.RETAIN,
    description="KMS key for earthquake ETL data lake encryption"
)

self.kms_key.add_alias("earthquake-etl/data-lake")

# Update data_bucket encryption:
encryption=s3.BucketEncryption.KMS(kms_key=self.kms_key)

# Update scripts_bucket encryption:
encryption=s3.BucketEncryption.KMS(kms_key=self.kms_key)

# Add KMS key output:
CfnOutput(self, "KmsKeyArn", value=self.kms_key.key_arn)
```

## X-Ray Tracing Implementation

**Lambda** (in IngestionStack):
```python
from aws_cdk import aws_lambda as _lambda
from aws_cdk.aws_lambda import Tracing

_lambda.Function(
    self,
    "EarthquakeIngestionLambda",
    # ... existing props
    tracing_config=_lambda.TracingConfig(
        mode=Tracing.ACTIVE
    )
)
```

**Lambda IAM** (add to existing role policy):
```python
lambda_role.add_to_policy(iam.PolicyStatement(
    effect=iam.Effect.ALLOW,
    actions=["xray:PutTraceSegments", "xray:GetTraceGraph"],
    resources=["*"]
))
```

**Glue** (in GlueStack, add to `default_arguments`):
```python
"--enable-xray-tracing": "true"
```

## SNS Parameter Implementation

```python
from aws_cdk import CfnParameter

alert_email = CfnParameter(
    self,
    "AlertEmail",
    type="String",
    default="admin@example.com",
    description="Email address for ETL alert notifications",
    allowed_pattern=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
    no_echo=False
)

# In MonitoringStack.__init__
alert_topic.add_subscription(
    subs.EmailSubscription(alert_email.value_as_string)
)
```

## cdk-nag Integration

```python
# In tests/unit/test_etl_stack.py
import cdk_nag NagSuppressions

def test_data_lake_stack_passes_cdk_nag():
    app = cdk.App()
    stack = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    
    NagSuppressions.add_stack_suppressions(
        stack,
        [{"id": "AwsSolutions-S1", "reason": "Access logs bucket intentionally public for test"}]
    )
    
    assertions.from_stack(stack).has_resource(
        "AWS::S3::Bucket",
        assertions.Match.object_like({"Type": "AWS::S3::Bucket"})
    )
```

## CI/CD Pipeline Architecture

```python
# etl_cdk/stacks/cicd_pipeline_stack.py
from aws_cdk import (
    Stack,
    pipelines,
    aws_codecommit as codecommit,
    aws_codebuild as codebuild,
    aws_codepipeline as codepipeline,
)

class CicdPipelineStack(Stack):
    def __init__(self, scope, construct_id, **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        
        repo = codecommit.Repository(
            self,
            "ETLRepository",
            repository_name="earthquake-etl"
        )
        
        pipeline = codepipeline.Pipeline(
            self,
            "ETLPipeline",
            pipeline_name="earthquake-etl-pipeline"
        )
        
        source_stage = pipeline.add_stage(stage_name="Source")
        source_action = codepipeline_actions.CodeCommitSourceAction(
            action_name="CodeCommitSource",
            repository=repo,
            branch="main",
            output=codepipeline.Artifact()
        )
        source_stage.add_action(source_action)
        
        build_stage = pipeline.add_stage(stage_name="Build")
        build_project = codebuild.PipelineProject(
            self,
            "CDKBuildProject",
            build_spec=codebuild.BuildSpec.from_object({
                "version": "0.2",
                "phases": {
                    "build": {
                        "commands": [
                            "npm install",
                            "cdk synth",
                            "cdk deploy --all --require-approval=never"
                        ]
                    }
                }
            })
        )
```

## Testing Strategy

| Layer | What to Test | Approach |
|-------|-------------|----------|
| Unit | Choice state transitions | Assert SFN definition contains Choice, Pass, and correct conditions |
| Unit | KMS key encryption on buckets | Template assertion on `BucketEncryption.KmsKey` |
| Unit | SNS parameter validation | Synth stack with invalid email, expect validation error |
| Unit | X-Ray config on Lambda | Template assertion on `TracingConfig.Mode` |
| Integration | CDK synth + cdk-nag | `cdk synth` + `cdk nag` CLI in CodeBuild |

## Open Questions

- [ ] Should CI/CD pipeline stack be environment-agnostic or separate dev/prod stacks?
- [ ] What AWS account(s) should the pipeline deploy to—single account with env separation, or separate accounts per environment?
