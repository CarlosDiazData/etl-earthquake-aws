import os
import sys
import aws_cdk as cdk
from aws_cdk import Environment
from etl_cdk.stacks.data_lake_stack import DataLakeStack
from etl_cdk.stacks.ingestion_stack import IngestionStack
from etl_cdk.stacks.glue_stack import GlueStack
from etl_cdk.stacks.orchestration_stack import OrchestrationStack
from etl_cdk.stacks.monitoring_stack import MonitoringStack

APP_NAME = "earthquake-etl"

# ENV_NAME from CDK context (passed by CI/CD) or environment variable or CLI argument
# Usage: cdk deploy --context ENV_NAME=dev
app = cdk.App()

_env_from_context = app.node.try_get_context("env_name")
if _env_from_context:
    ENV_NAME = _env_from_context
elif len(sys.argv) > 1:
    ENV_NAME = sys.argv[1]
else:
    ENV_NAME = os.getenv("ENV_NAME", "dev")

# Environment configuration
# In CI/CD (CodeBuild): CDK auto-detects account/region from IAM role
# Locally: uses AWS_ACCOUNT/AWS_REGION env vars or defaults
AWS_ACCOUNT = os.environ.get("CDK_DEFAULT_ACCOUNT")
AWS_REGION = os.environ.get("CDK_DEFAULT_REGION", "us-east-1")

if AWS_ACCOUNT:
    # Explicit env when running locally (with credentials)
    deploy_env = Environment(account=AWS_ACCOUNT, region=AWS_REGION)
else:
    # Environment-agnostic for CI/CD (uses IAM role's account)
    deploy_env = None

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

# Optionally deploy CI/CD pipeline stack
# Usage: cdk deploy --context include_pipeline=true
if app.node.try_get_context("include_pipeline") == "true":
    from etl_cdk.stacks.cicd_pipeline_stack import CicdPipelineStack
    CicdPipelineStack(
        app,
        f"{APP_NAME}-cicd-pipeline",
        env=deploy_env,
    )

app.synth()
