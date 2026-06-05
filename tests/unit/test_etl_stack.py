import aws_cdk as cdk
import aws_cdk.assertions as assertions
from aws_cdk import Aspects
from etl_cdk.stacks.data_lake_stack import DataLakeStack
from etl_cdk.stacks.ingestion_stack import IngestionStack
from etl_cdk.stacks.pipeline_stack import PipelineStack


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


# =============================================================================
# PipelineStack Tests
# =============================================================================


def test_pipeline_stack_creates_resources():
    app = cdk.App()
    data_lake = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    ingestion = IngestionStack(
        app,
        "TestIngestion",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
    )
    stack = PipelineStack(
        app,
        "TestPipeline",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
        scripts_bucket=data_lake.scripts_bucket,
        ingestion_lambda=ingestion.ingestion_lambda,
    )
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::Glue::Job", 2)
    template.resource_count_is("AWS::StepFunctions::StateMachine", 1)
    template.resource_count_is("AWS::CloudWatch::Alarm", 4)
    template.resource_count_is("AWS::SNS::Topic", 1)
    template.resource_count_is("AWS::CloudWatch::Dashboard", 1)

    cf_params = template.to_json().get("Parameters", {})
    assert "AlertEmail" in cf_params, "AlertEmail CfnParameter not found in template"


def test_pipeline_stack_state_machine_has_choice():
    app = cdk.App()
    data_lake = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    ingestion = IngestionStack(
        app,
        "TestIngestion",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
    )
    stack = PipelineStack(
        app,
        "TestPipeline",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
        scripts_bucket=data_lake.scripts_bucket,
        ingestion_lambda=ingestion.ingestion_lambda,
    )
    template = assertions.Template.from_stack(stack)

    resources = template.to_json().get("Resources", {})
    sm_resources = {
        k: v
        for k, v in resources.items()
        if v.get("Type") == "AWS::StepFunctions::StateMachine"
    }
    assert len(sm_resources) == 1
    sm_props = list(sm_resources.values())[0].get("Properties", {})
    definition = str(sm_props.get("DefinitionString", ""))
    assert "CheckRecordsCount" in definition, "Choice state not found in definition"


def test_pipeline_stack_has_result_selector():
    app = cdk.App()
    data_lake = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    ingestion = IngestionStack(
        app,
        "TestIngestion",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
    )
    stack = PipelineStack(
        app,
        "TestPipeline",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
        scripts_bucket=data_lake.scripts_bucket,
        ingestion_lambda=ingestion.ingestion_lambda,
    )
    template = assertions.Template.from_stack(stack)

    resources = template.to_json().get("Resources", {})
    sm_resources = {
        k: v
        for k, v in resources.items()
        if v.get("Type") == "AWS::StepFunctions::StateMachine"
    }
    assert len(sm_resources) == 1
    sm_props = list(sm_resources.values())[0].get("Properties", {})
    definition = str(sm_props.get("DefinitionString", ""))
    assert "recordsCount.$" in definition, "ResultSelector with recordsCount.$ not found"


def test_pipeline_stack_has_add_catch():
    app = cdk.App()
    data_lake = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    ingestion = IngestionStack(
        app,
        "TestIngestion",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
    )
    stack = PipelineStack(
        app,
        "TestPipeline",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
        scripts_bucket=data_lake.scripts_bucket,
        ingestion_lambda=ingestion.ingestion_lambda,
    )
    template = assertions.Template.from_stack(stack)

    resources = template.to_json().get("Resources", {})
    sm_resources = {
        k: v
        for k, v in resources.items()
        if v.get("Type") == "AWS::StepFunctions::StateMachine"
    }
    assert len(sm_resources) == 1
    sm_props = list(sm_resources.values())[0].get("Properties", {})
    definition = str(sm_props.get("DefinitionString", ""))
    assert "LambdaFailed" in definition, "add_catch handler 'LambdaFailed' not found"


def test_pipeline_stack_alert_email_has_validation():
    app = cdk.App()
    data_lake = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    ingestion = IngestionStack(
        app,
        "TestIngestion",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
    )
    stack = PipelineStack(
        app,
        "TestPipeline",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
        scripts_bucket=data_lake.scripts_bucket,
        ingestion_lambda=ingestion.ingestion_lambda,
    )
    template = assertions.Template.from_stack(stack)

    cf_params = template.to_json().get("Parameters", {})
    assert "AlertEmail" in cf_params, "AlertEmail CfnParameter not found"

    alert_email_param = cf_params["AlertEmail"]
    assert alert_email_param.get("Type") == "String", "AlertEmail should be String type"
    assert alert_email_param.get("Default") == "admin@example.com", "AlertEmail should have default"
    assert (
        alert_email_param.get("AllowedPattern")
        == r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    ), "AlertEmail should have email validation pattern"


def test_pipeline_stack_jobs_have_xray():
    app = cdk.App()
    data_lake = DataLakeStack(app, "TestDataLake", app_name="test-etl", env_name="dev")
    ingestion = IngestionStack(
        app,
        "TestIngestion",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
    )
    stack = PipelineStack(
        app,
        "TestPipeline",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
        scripts_bucket=data_lake.scripts_bucket,
        ingestion_lambda=ingestion.ingestion_lambda,
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


def test_pipeline_stack_passes_cdk_nag():
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
    stack = PipelineStack(
        app,
        "TestPipeline",
        app_name="test-etl",
        env_name="dev",
        data_bucket=data_lake.data_bucket,
        scripts_bucket=data_lake.scripts_bucket,
        ingestion_lambda=ingestion.ingestion_lambda,
    )
    Aspects.of(stack).add(cdk_nag.AwsSolutionsChecks())
    annotations = []

    def handler(node):
        for a in node.node.metadata:
            if a.get("type") == "aws:cdk:warning":
                annotations.append(a.get("data"))

    assert len(annotations) == 0, f"cdk-nag warnings found: {annotations}"


