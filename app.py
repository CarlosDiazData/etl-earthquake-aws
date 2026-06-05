import os
import aws_cdk as cdk
from aws_cdk import Environment
from etl_cdk.stacks.data_lake_stack import DataLakeStack
from etl_cdk.stacks.ingestion_stack import IngestionStack
from etl_cdk.stacks.pipeline_stack import PipelineStack

APP_NAME = "earthquake-etl"

# ENV_NAME from CDK context (passed by GitHub Actions CI/CD)
# Usage: cdk deploy --context ENV_NAME=dev
app = cdk.App()

ENV_NAME = app.node.try_get_context("env_name") or os.getenv("ENV_NAME", "dev")

# Environment configuration
# In GitHub Actions: CDK auto-detects account/region from OIDC role
# Locally: uses CDK_DEFAULT_ACCOUNT/CDK_DEFAULT_REGION env vars
AWS_ACCOUNT = os.environ.get("CDK_DEFAULT_ACCOUNT")
AWS_REGION = os.environ.get("CDK_DEFAULT_REGION", "us-east-1")

if AWS_ACCOUNT:
    deploy_env = Environment(account=AWS_ACCOUNT, region=AWS_REGION)
else:
    deploy_env = None  # CI/CD: uses IAM role's account

data_lake = DataLakeStack(
    app,
    f"{APP_NAME}-data-lake-{ENV_NAME}",
    app_name=APP_NAME,
    env_name=ENV_NAME,
    env=deploy_env,
)

ingestion = IngestionStack(
    app,
    f"{APP_NAME}-ingestion-{ENV_NAME}",
    app_name=APP_NAME,
    env_name=ENV_NAME,
    data_bucket=data_lake.data_bucket,
    env=deploy_env,
)
ingestion.add_dependency(data_lake)

pipeline = PipelineStack(
    app,
    f"{APP_NAME}-pipeline-{ENV_NAME}",
    app_name=APP_NAME,
    env_name=ENV_NAME,
    data_bucket=data_lake.data_bucket,
    scripts_bucket=data_lake.scripts_bucket,
    ingestion_lambda=ingestion.ingestion_lambda,
    env=deploy_env,
)
pipeline.add_dependency(data_lake)
pipeline.add_dependency(ingestion)

app.synth()
