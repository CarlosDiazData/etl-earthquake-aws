import aws_cdk as cdk
import aws_cdk.assertions as assertions
from etl_cdk.stacks.cicd_pipeline_stack import CicdPipelineStack


def test_cicd_pipeline_stack_synthesizes():
    app = cdk.App()
    stack = CicdPipelineStack(app, "TestCicdPipeline")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::CodeCommit::Repository", 1)
    template.resource_count_is("AWS::CodePipeline::Pipeline", 1)
    template.resource_count_is("AWS::CodeBuild::Project", 1)


def test_cicd_pipeline_has_repository():
    app = cdk.App()
    stack = CicdPipelineStack(app, "TestCicdPipeline")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties(
        "AWS::CodeCommit::Repository",
        {
            "RepositoryName": "earthquake-etl",
        },
    )


def test_cicd_pipeline_has_pipeline():
    app = cdk.App()
    stack = CicdPipelineStack(app, "TestCicdPipeline")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties(
        "AWS::CodePipeline::Pipeline",
        {
            "Name": "earthquake-etl-pipeline",
        },
    )


def test_cicd_pipeline_has_codebuild_project():
    app = cdk.App()
    stack = CicdPipelineStack(app, "TestCicdPipeline")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::CodeBuild::Project", 1)
