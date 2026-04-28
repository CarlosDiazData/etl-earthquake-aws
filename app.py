import os
import aws_cdk as cdk
from aws_cdk import Environment
from etl_cdk.stacks.data_lake_stack import DataLakeStack
from etl_cdk.stacks.ingestion_stack import IngestionStack
from etl_cdk.stacks.glue_stack import GlueStack
from etl_cdk.stacks.orchestration_stack import OrchestrationStack
from etl_cdk.stacks.monitoring_stack import MonitoringStack

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

glue = GlueStack(
    app,
    f"{APP_NAME}-glue-{ENV_NAME}",
    app_name=APP_NAME,
    env_name=ENV_NAME,
    data_bucket=data_lake.data_bucket,
    scripts_bucket=data_lake.scripts_bucket,
    env=deploy_env,
)
glue.add_dependency(data_lake)

orchestration = OrchestrationStack(
    app,
    f"{APP_NAME}-orchestration-{ENV_NAME}",
    app_name=APP_NAME,
    env_name=ENV_NAME,
    ingestion_lambda=ingestion.ingestion_lambda,
    bronze_to_silver_job=glue.bronze_to_silver_job,
    silver_to_gold_job=glue.silver_to_gold_job,
    data_bucket=data_lake.data_bucket,
    env=deploy_env,
)
orchestration.add_dependency(ingestion)
orchestration.add_dependency(glue)

MonitoringStack(
    app,
    f"{APP_NAME}-monitoring-{ENV_NAME}",
    app_name=APP_NAME,
    env_name=ENV_NAME,
    data_bucket=data_lake.data_bucket,
    bronze_to_silver_job=glue.bronze_to_silver_job,
    silver_to_gold_job=glue.silver_to_gold_job,
    state_machine=orchestration.state_machine,
    env=deploy_env,
)

app.synth()
