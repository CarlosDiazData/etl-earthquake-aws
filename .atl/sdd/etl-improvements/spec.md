# Delta Spec: etl-improvements

## ADDED Requirements

### Requirement: step-functions-choice

The system SHALL add a Choice state after the ingestion Lambda that branches based on `$.recordsCount`. When recordsCount equals 0, the pipeline SHALL terminate successfully without invoking downstream Glue jobs.

#### Scenario: Zero records ingested

- GIVEN the ingestion Lambda returns `{"recordsCount": 0, ...}`
- WHEN the Step Functions execution reaches the Choice state
- THEN the state machine SHALL transition to a Pass state labeled "NoRecordsSkipped"
- AND the state machine SHALL end in SUCCEEDED state
- AND the bronze-to-silver job SHALL NOT be invoked

#### Scenario: Records ingested

- GIVEN the ingestion Lambda returns `{"recordsCount": 42, ...}`
- WHEN the Step Functions execution reaches the Choice state
- THEN the state machine SHALL transition to the bronze-to-silver Glue task
- AND the full ETL chain SHALL execute end-to-end

### Requirement: cdk-nag-scanning

The system SHALL integrate cdk-nag into the test suite to enforce AWS Foundational Security Best Practices. All stacks MUST pass cdk-nag validation with no errors.

#### Scenario: All stacks pass cdk-nag

- GIVEN `pytest tests/` is executed
- THEN cdk-nag MUST scan all stacks (DataLakeStack, IngestionStack, GlueStack, OrchestrationStack, MonitoringStack)
- AND the scan MUST report 0 errors
- AND warnings MAY be suppressed with documented justification

#### Scenario: New security control fails

- GIVEN a stack introduces a new AWS::S3::Bucket policy without encryption
- WHEN `pytest tests/` is executed
- THEN cdk-nag MUST report a FAILED assertion
- AND the test MUST fail

### Requirement: cicd-pipeline

The system SHALL deploy a CodePipeline CI/CD pipeline that automatically deploys CDK stacks to dev on PR merge and to prod on main branch merge.

#### Scenario: PR merged to develop branch

- GIVEN a pull request is merged to the `develop` branch
- WHEN CodePipeline detects the merge
- THEN the pipeline SHALL trigger a CDK synth and deploy to the `dev` environment
- AND the pipeline SHALL emit a CloudWatch event with status "dev-deploy succeeded"

#### Scenario: PR merged to main branch

- GIVEN a pull request is merged to the `main` branch
- WHEN CodePipeline detects the merge
- THEN the pipeline SHALL trigger a CDK synth and deploy to the `prod` environment
- AND the pipeline SHALL emit a CloudWatch event with status "prod-deploy succeeded"

#### Scenario: Pipeline bootstrap failure

- GIVEN CDK bootstrap has not been run in the target account
- WHEN CodePipeline attempts to deploy
- THEN the pipeline SHALL halt with a resource bootstrap error
- AND the pipeline status SHALL show "failed"

---

## MODIFIED Requirements

### Requirement: data-bucket-encryption

The system MUST encrypt all S3 buckets with KMS Customer Master Keys (CMK) instead of S3-managed encryption. The KMS key MUST enable rotation without re-deployment. Both the data bucket and scripts bucket MUST use the same KMS key for consistent data-at-rest encryption.

(Previously: Buckets used `BucketEncryption.S3_MANAGED`)

#### Scenario: Data bucket uses KMS encryption

- GIVEN the DataLakeStack is synthesized
- WHEN the template is generated
- THEN the `EarthquakeDataBucket` resource MUST have `BucketEncryption` set to `Kms`
- AND the KMS key ARN MUST be stored as a stack output

#### Scenario: Scripts bucket uses same KMS key

- GIVEN the DataLakeStack is synthesized
- WHEN the template is generated
- THEN the `ScriptsBucket` resource MUST reference the same KMS key as the data bucket
- AND the bucket policy MUST grant Glue service role decrypt permissions

#### Scenario: KMS key rotation is enabled

- GIVEN the DataLakeStack is synthesized
- THEN the KMS key MUST have `enable_key_rotation: true` in its properties
- AND the key policy MUST allow the deployment role to use the key

### Requirement: monitoring-alerts

The system SHALL accept an SNS alert email address as a CloudFormation stack parameter with default `admin@example.com`. The parameter MUST be validated as a non-empty string.

(Previously: Email was hardcoded to `admin@example.com`)

#### Scenario: Default email used when no parameter provided

- GIVEN MonitoringStack is instantiated without an explicit `alert_email` parameter
- THEN the SNS subscription SHALL use `admin@example.com`
- AND CloudFormation deployment SHALL succeed

#### Scenario: Custom email provided via parameter

- GIVEN MonitoringStack is instantiated with `alert_email="ops@company.com"`
- THEN the SNS subscription SHALL use `ops@company.com`
- AND CloudFormation deployment SHALL succeed

#### Scenario: Empty email rejected

- GIVEN MonitoringStack is instantiated with `alert_email=""`
- THEN CDK synth SHALL fail with a validation error

### Requirement: xray-tracing

The system SHALL enable X-Ray tracing on all Lambda functions and Glue jobs. Lambda functions MUST set `tracing_config.mode = Tracing.ACTIVE`. Glue jobs MUST pass `--enable-xray-tracing` via job arguments.

(Previously: Only Step Functions had tracing enabled)

#### Scenario: Lambda has active X-Ray tracing

- GIVEN IngestionStack is synthesized
- WHEN the `IngestionLambda` resource is created
- THEN `TracingConfig.Mode` MUST equal `Tracing.ACTIVE`
- AND the Lambda execution role MUST have `xray:PutTraceSegments` permissions

#### Scenario: Glue job passes X-Ray argument

- GIVEN GlueStack is synthesized
- WHEN the bronze-to-silver job is created
- THEN `DefaultArguments` MUST include `"--enable-xray-tracing": "true"`
- AND the silver-to-gold job MUST also include the argument

### Requirement: glue-bookmarks

The system SHALL enable Glue job bookmarks on the bronze-to-silver job to support incremental processing. The bookmark configuration MUST be set to `job-bookmark-enable`.

(Previously: Bookmark was enabled but bookmark handling logic was not present in the ETL script)

#### Scenario: Bronze-to-silver job has bookmarks enabled

- GIVEN GlueStack is synthesized
- WHEN the `bronze_to_silver_job` resource is created
- THEN `DefaultArguments["--job-bookmark-option"]` MUST equal `"job-bookmark-enable"`
- AND the job bookmark policy in the Glue script MUST read and update bookmarks per partition

#### Scenario: Incremental run skips processed records

- GIVEN bronze layer has 100 records from a previous run with bookmarked state
- WHEN the bronze-to-silver job runs with bookmarks enabled
- THEN only NEW records (after the bookmarked offset) SHALL be processed
- AND the job run SHALL complete without re-processing existing records

---

## REMOVED Requirements

None.

---

## Coverage Summary

| Domain | Type | Requirements | Scenarios |
|--------|------|-------------|-----------|
| step-functions-choice | Added | 1 | 2 |
| cdk-nag-scanning | Added | 1 | 2 |
| cicd-pipeline | Added | 1 | 3 |
| data-bucket-encryption | Modified | 1 | 3 |
| monitoring-alerts | Modified | 1 | 3 |
| xray-tracing | Modified | 1 | 2 |
| glue-bookmarks | Modified | 1 | 2 |

**Happy paths**: 8 covered
**Edge cases**: 5 covered
**Error states**: 2 covered (validation, bootstrap)
