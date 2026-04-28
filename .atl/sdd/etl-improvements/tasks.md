# Tasks: etl-improvements

## Phase 1: Infrastructure (KMS + X-Ray IAM)

- [x] 1.1 In `data_lake_stack.py`: Import `aws_kms as kms` and create `DataLakeKmsKey` with `enable_key_rotation=True`, `removal_policy=RetentionPolicy.RETAIN`, add alias `earthquake-etl/data-lake`
- [x] 1.2 In `data_lake_stack.py`: Add `CfnOutput` for `KmsKeyArn` stack output
- [x] 1.3 In `ingestion_stack.py`: Add `xray:PutTraceSegments` and `xray:GetTraceGraph` to Lambda execution role policy

## Phase 2: Core Implementation

- [x] 2.1 In `data_lake_stack.py`: Update `EarthquakeDataBucket` encryption to `s3.BucketEncryption.KMS(kms_key=self.kms_key)`
- [x] 2.2 In `data_lake_stack.py`: Update `ScriptsBucket` encryption to use same `kms_key`, add Glue service role decrypt permission to bucket policy
- [x] 2.3 In `ingestion_stack.py`: Add `tracing_config=_lambda.TracingConfig(mode=Tracing.ACTIVE)` to `EarthquakeIngestionLambda`
- [x] 2.4 In `glue_stack.py`: Add `"--enable-xray-tracing": "true"` to `bronze_to_silver_job` DefaultArguments
- [x] 2.5 In `glue_stack.py`: Add `"--enable-xray-tracing": "true"` and `"--job-bookmark-option": "job-bookmark-enable"` to `silver_to_gold_job` DefaultArguments
- [x] 2.6 In `monitoring_stack.py`: Add `CfnParameter` for `alert_email` with `default="admin@example.com"`, `allowed_pattern` for email validation
- [x] 2.7 In `monitoring_stack.py`: Update SNS subscription to use `alert_email.value_as_string`
- [x] 2.8 In `orchestration_stack.py`: Add `sfn.Choice` state "CheckRecordsCount" after ingestion Lambda
- [x] 2.9 In `orchestration_stack.py`: Add `sfn.Pass` state "NoRecordsSkipped" for zero records path
- [x] 2.10 In `orchestration_stack.py`: Add choice rules: `Condition.number_equals("$.recordsCount", 0)` → NoRecordsSkipped, `Condition.number_greater_than("$.recordsCount", 0)` → bronze_to_silver_task

## Phase 3: CI/CD Pipeline

- [x] 3.1 Create `etl_cdk/stacks/cicd_pipeline_stack.py` with `CicdPipelineStack` class extending `Stack`
- [x] 3.2 In `cicd_pipeline_stack.py`: Create `codecommit.Repository` "ETLRepository" with `repository_name="earthquake-etl"`
- [x] 3.3 In `cicd_pipeline_stack.py`: Create `codepipeline.Pipeline` "ETLPipeline"
- [x] 3.4 In `cicd_pipeline_stack.py`: Add Source stage with `CodeCommitSourceAction` on "main" branch
- [x] 3.5 In `cicd_pipeline_stack.py`: Add Build stage with `CodeBuildProject` running `cdk synth` and `cdk deploy --all --require-approval=never`
- [x] 3.6 Create `tests/unit/test_cicd_pipeline.py` with tests for pipeline stack synthesis

## Phase 4: Testing (cdk-nag)

- [x] 4.1 In `tests/unit/test_etl_stack.py`: Import `cdk_nag` and `NagSuppressions`
- [x] 4.2 Add `test_data_lake_stack_passes_cdk_nag` asserting 0 errors, suppress `AwsSolutions-S1` with documented reason
- [x] 4.3 Add `test_ingestion_stack_passes_cdk_nag` asserting 0 errors
- [x] 4.4 Add `test_glue_stack_passes_cdk_nag` asserting 0 errors
- [x] 4.5 Add `test_orchestration_stack_passes_cdk_nag` asserting 0 errors
- [x] 4.6 Add `test_monitoring_stack_passes_cdk_nag` asserting 0 errors

## Phase 5: Integration Tests

- [x] 5.1 Verify `sfn.Choice` state exists in OrchestrationStack template with correct conditions
- [x] 5.2 Verify S3 buckets have `BucketEncryption.KmsKey` in DataLakeStack template
- [x] 5.3 Verify Lambda `TracingConfig.Mode` equals `Tracing.ACTIVE` in IngestionStack template
- [x] 5.4 Verify Glue jobs have `"--enable-xray-tracing": "true"` in GlueStack template
- [ ] 5.5 Synth stack with empty email `alert_email=""` and verify CDK synth fails with validation error
