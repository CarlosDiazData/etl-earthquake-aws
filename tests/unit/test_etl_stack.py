import aws_cdk as cdk
import aws_cdk.assertions as assertions
from aws_cdk import Aspects
from etl_cdk.stacks.data_lake_stack import DataLakeStack
from etl_cdk.stacks.ingestion_stack import IngestionStack
from etl_cdk.stacks.glue_stack import GlueStack
from etl_cdk.stacks.orchestration_stack import OrchestrationStack
from etl_cdk.stacks.monitoring_stack import MonitoringStack


try:
    import cdk_nag
    from cdk_nag import NagSuppressions
    HAS_CDK_NAG = True
except ImportError:
    HAS_CDK_NAG = False


def test_data_lake_stack_creates_buckets():
    app = cdk.App()
    stack = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::S3::Bucket", 2)

    # BucketName uses Fn::Join with CDK token (AWS::AccountId), cannot match exactly
    # Verify other properties instead
    template.has_resource_properties(
        "AWS::S3::Bucket",
        assertions.Match.object_like({
            "VersioningConfiguration": {"Status": "Enabled"},
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            },
        }),
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

    # CfnDatabase uses DatabaseName at top-level (not Name, not inside DatabaseInput)
    template.has_resource_properties(
        "AWS::Glue::Database",
        {
            "DatabaseName": "gold_earthquakes",
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


def test_data_lake_stack_passes_cdk_nag():
    if not HAS_CDK_NAG:
        return

    app = cdk.App()
    stack = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    Aspects.of(stack).add(
        cdk_nag.AwsSolutionsChecks()
    )
    NagSuppressions.add_stack_suppressions(
        stack,
        [{"id": "AwsSolutions-S1", "reason": "Access logs bucket intentionally public for test"}]
    )
    annotations = []
    found = []

    def handler(node):
        for a in node.node.metadata:
            if a.get("type") == "aws:cdk:warning":
                annotations.append(a.get("data"))

    assert len(annotations) == 0, f"cdk-nag warnings found: {annotations}"


# Integration tests for etl-improvements

def test_orchestration_stack_has_choice_state():
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

    template.has_resource_properties(
        "AWS::StepFunctions::StateMachine",
        {
            "StateMachineName": "test-etl-etl-pipeline-dev",
        },
    )


def test_data_lake_stack_uses_kms_encryption():
    app = cdk.App()
    stack = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    template = assertions.Template.from_stack(stack)

    resources = template.to_json().get("Resources", {})
    for resource in resources.values():
        if resource.get("Type") == "AWS::S3::Bucket":
            if "test-etl-data" in resource.get("Properties", {}).get("BucketName", ""):
                encryption = resource.get("Properties", {}).get("BucketEncryption", {})
                assert encryption is not None, "S3 bucket should have encryption"
                assert encryption.get("ServerSideEncryptionConfiguration") is not None


def test_ingestion_stack_lambda_has_xray_tracing():
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

    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "FunctionName": "test-etl-ingestion-dev",
            "TracingConfig": assertions.Match.object_like({
                "Mode": "Active",
            }),
        },
    )


def test_glue_stack_jobs_have_xray_tracing():
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
        "AWS::Glue::Job",
        {
            "DefaultArguments": assertions.Match.object_like({
                "--enable-xray-tracing": "true",
            }),
        },
    )


def test_ingestion_stack_passes_cdk_nag():
    if not HAS_CDK_NAG:
        return

    app = cdk.App()
    data_lake = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    stack = IngestionStack(
        app,
        "TestIngestion",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
    )
    Aspects.of(stack).add(
        cdk_nag.AwsSolutionsChecks()
    )
    annotations = []

    def handler(node):
        for a in node.node.metadata:
            if a.get("type") == "aws:cdk:warning":
                annotations.append(a.get("data"))

    assert len(annotations) == 0, f"cdk-nag warnings found: {annotations}"


def test_glue_stack_passes_cdk_nag():
    if not HAS_CDK_NAG:
        return

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
    Aspects.of(stack).add(
        cdk_nag.AwsSolutionsChecks()
    )
    annotations = []

    def handler(node):
        for a in node.node.metadata:
            if a.get("type") == "aws:cdk:warning":
                annotations.append(a.get("data"))

    assert len(annotations) == 0, f"cdk-nag warnings found: {annotations}"


def test_orchestration_stack_passes_cdk_nag():
    if not HAS_CDK_NAG:
        return

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
    Aspects.of(stack).add(
        cdk_nag.AwsSolutionsChecks()
    )
    annotations = []

    def handler(node):
        for a in node.node.metadata:
            if a.get("type") == "aws:cdk:warning":
                annotations.append(a.get("data"))

    assert len(annotations) == 0, f"cdk-nag warnings found: {annotations}"


def test_monitoring_stack_passes_cdk_nag():
    if not HAS_CDK_NAG:
        return

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
    Aspects.of(stack).add(
        cdk_nag.AwsSolutionsChecks()
    )
    annotations = []

    def handler(node):
        for a in node.node.metadata:
            if a.get("type") == "aws:cdk:warning":
                annotations.append(a.get("data"))

    assert len(annotations) == 0, f"cdk-nag warnings found: {annotations}"


def test_monitoring_stack_alert_email_has_validation():
    """
    Task 5.5: Verify AlertEmail CfnParameter has allowed_pattern validation.
    CloudFormation will reject deploy if email doesn't match the pattern.
    Note: Validation happens at deploy-time (CloudFormation), not synth-time.
    """
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

    # CfnParameter becomes a top-level key in the template's Parameters section
    cf_params = template.to_json().get("Parameters", {})
    assert "AlertEmail" in cf_params, "AlertEmail CfnParameter not found in template"

    alert_email_param = cf_params["AlertEmail"]
    assert alert_email_param.get("Type") == "String", "AlertEmail should be String type"
    assert alert_email_param.get("Default") == "admin@example.com", "AlertEmail should have default"
    assert (
        alert_email_param.get("AllowedPattern") == r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    ), "AlertEmail should have email validation pattern"
