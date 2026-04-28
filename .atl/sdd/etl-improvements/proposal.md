# Proposal: etl-improvements

## Intent

Productionize the Earthquake ETL pipeline by addressing operational inefficiencies, security gaps, and deployment gaps. The Lambda currently executes Glue jobs unconditionally—even when returning zero records—wasting resources. Security compliance and observability are incomplete, and there's no automated deployment pipeline.

## Scope

### In Scope
- Add Choice state to Step Functions to skip downstream jobs when no records ingested
- Replace hardcoded `admin@example.com` SNS subscription with parameterized email
- Migrate S3 encryption from SSE-S3 to KMS with CMK for data-at-rest encryption
- Add cdk-nag for AWS Foundational Security Best Practices compliance scanning
- Implement CI/CD pipeline with CodePipeline for automated CDK deployments
- Enable X-Ray tracing on Lambda and Glue jobs (Step Functions already has it)

### Out of Scope
- Re-architecting the ETL logic (bronze/silver/gold transformations)
- Adding new data sources or sinks
- Performance optimization of Spark jobs
- Multi-region deployment

## Capabilities

### New Capabilities
- `step-functions-choice`: Conditional branching based on Lambda ingestion result
- `cdk-nag-scanning`: Automated security compliance in CI/CD
- `cicd-pipeline`: Automated deployment pipeline for CDK stacks

### Modified Capabilities
- `data-bucket-encryption`: Upgrade from SSE-S3 to KMS CMK (existing)
- `monitoring-alerts`: Parameterize SNS email subscription (existing)
- `xray-tracing`: Enable on Lambda and Glue, not just Step Functions (existing)

## Approach

Incremental improvements with backward compatibility:
1. Add Choice state after Lambda invocation—branch to End if `$.recordsCount == 0`
2. Convert SNS email to stack parameter with default for backward compat
3. Create KMS key in DataLakeStack, apply to both buckets
4. Add cdk-nag to tests with suppression for known gaps
5. Bootstrap CI/CD with CodePipeline, CodeBuild, GitHub source
6. Enable X-Ray on Lambda via `tracing_config` and on Glue via job arguments

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `etl_cdk/stacks/orchestration_stack.py` | Modified | Add Choice state, enable Lambda X-Ray |
| `etl_cdk/stacks/monitoring_stack.py` | Modified | Email as parameter |
| `etl_cdk/stacks/data_lake_stack.py` | Modified | KMS encryption, Lambda X-Ray |
| `tests/unit/test_etl_stack.py` | Modified | Add cdk-nag assertions |
| `app.py` | Modified | Bootstrap CI/CD pipeline |
| New: `etl_cdk/pipeline/` | New | CI/CD pipeline stack |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| KMS key rotation breaks Glue job access | Low | Test in dev first, use dual-key strategy |
| Choice state breaks existing deployments | Medium | Maintain `else` branch that runs jobs (backward compat) |
| cdk-nag failures block CI | Med | Use suppressions with justification, target 100% compliance |

## Rollback Plan

- **CodePipeline**: Disable stage in CodePipeline console, set CI/CD stack to RETAIN
- **KMS migration**: Switch bucket encryption back to SSE-S3 via parameter toggle
- **Choice state**: Redeploy with original linear definition from git history
- **cdk-nag**: Remove `NagSuppressions` and re-run synth

## Dependencies

- GitHub repository with branch protection (main + develop)
- Existing AWS account with admin permissions for CDK bootstrap
- KMS key admin role separate from deployment role

## Success Criteria

- [ ] Step Functions branch skips Glue jobs when Lambda returns 0 records
- [ ] SNS email configurable via CloudFormation parameter
- [ ] S3 buckets use KMS encryption; keys rotatable without re-deployment
- [ ] `pytest` runs cdk-nag with 0 failures on all stacks
- [ ] CI/CD pipeline deploys to dev on PR merge, to prod on main merge
- [ ] Lambda and Glue show traces in X-Ray console