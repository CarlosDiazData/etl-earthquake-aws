import aws_cdk as cdk
import aws_cdk.assertions as assertions
from etl_cdk.stacks.data_lake_stack import DataLakeStack
from etl_cdk.stacks.ingestion_stack import IngestionStack
from etl_cdk.stacks.glue_stack import GlueStack
from etl_cdk.stacks.orchestration_stack import OrchestrationStack
from etl_cdk.stacks.monitoring_stack import MonitoringStack


def test_data_lake_stack_creates_buckets():
    app = cdk.App()
    stack = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::S3::Bucket", 2)

    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "BucketName": "test-etl-data-dev-123456789012",
            "VersioningConfiguration": {"Status": "Enabled"},
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            },
        },
    )


def test_data_lake_stack_has_lifecycle_rules():
    app = cdk.App()
    stack = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "LifecycleConfiguration": {
                "Rules": assertions.Match.array_with(
                    [
                        assertions.Match.object_like(
                            {
                                "Prefix": "bronze/",
                            }
                        ),
                    ]
                ),
            },
        },
    )


def test_ingestion_stack_creates_lambda():
    app = cdk.App()
    data_lake = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    stack = IngestionStack(
        app,
        "TestIngestion",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
    )
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::Lambda::Function", 1)

    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "FunctionName": "test-etl-ingestion-dev",
            "Runtime": "python3.12",
            "Handler": "api_to_bronze.lambda_handler",
            "Timeout": 300,
            "MemorySize": 512,
        },
    )


def test_ingestion_stack_creates_eventbridge_rule():
    app = cdk.App()
    data_lake = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    stack = IngestionStack(
        app,
        "TestIngestion",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
    )
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::Events::Rule", 1)


def test_glue_stack_creates_jobs():
    app = cdk.App()
    data_lake = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    stack = GlueStack(
        app,
        "TestGlue",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
        scripts_bucket=data_lake.scripts_bucket,
    )
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::Glue::Job", 2)

    template.has_resource_properties(
        "AWS::Glue::Job",
        {
            "GlueVersion": "5.0",
            "Command": {
                "PythonVersion": "3",
                "Name": "glueetl",
            },
        },
    )


def test_glue_stack_creates_database():
    app = cdk.App()
    data_lake = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    stack = GlueStack(
        app,
        "TestGlue",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
        scripts_bucket=data_lake.scripts_bucket,
    )
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties(
        "AWS::Glue::Database",
        {
            "DatabaseInput": {
                "Name": "gold_earthquakes",
            },
        },
    )


def test_orchestration_stack_creates_state_machine():
    app = cdk.App()
    data_lake = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    ingestion = IngestionStack(
        app,
        "TestIngestion",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
    )
    glue = GlueStack(
        app,
        "TestGlue",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
        scripts_bucket=data_lake.scripts_bucket,
    )
    stack = OrchestrationStack(
        app,
        "TestOrchestration",
        app_name="test-etl",
        env_name="dev",
        ingestion_lambda=ingestion.ingestion_lambda,
        bronze_to_silver_job=glue.bronze_to_silver_job,
        silver_to_gold_job=glue.silver_to_gold_job,
        data_bucket=data_lake.data_bucket,
    )
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::StepFunctions::StateMachine", 1)

    template.has_resource_properties(
        "AWS::StepFunctions::StateMachine",
        {
            "StateMachineName": "test-etl-etl-pipeline-dev",
        },
    )


def test_monitoring_stack_creates_alarms():
    app = cdk.App()
    data_lake = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    ingestion = IngestionStack(
        app,
        "TestIngestion",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
    )
    glue = GlueStack(
        app,
        "TestGlue",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
        scripts_bucket=data_lake.scripts_bucket,
    )
    orchestration = OrchestrationStack(
        app,
        "TestOrchestration",
        app_name="test-etl",
        env_name="dev",
        ingestion_lambda=ingestion.ingestion_lambda,
        bronze_to_silver_job=glue.bronze_to_silver_job,
        silver_to_gold_job=glue.silver_to_gold_job,
        data_bucket=data_lake.data_bucket,
    )
    stack = MonitoringStack(
        app,
        "TestMonitoring",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
        bronze_to_silver_job=glue.bronze_to_silver_job,
        silver_to_gold_job=glue.silver_to_gold_job,
        state_machine=orchestration.state_machine,
    )
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::CloudWatch::Alarm", 4)
    template.resource_count_is("AWS::SNS::Topic", 1)
    template.resource_count_is("AWS::CloudWatch::Dashboard", 1)
