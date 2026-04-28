import aws_cdk as cdk
import aws_cdk.assertions as assertions
from etl_cdk.stacks.cicd_pipeline_stack import CicdPipelineStack


def test_cicd_pipeline_stack_synthesizes():
    app = cdk.App()
    stack = CicdPipelineStack(app, "TestCicdPipeline")
    template = assertions.Template.from_stack(stack)

    # 1 repo shared by both pipelines
    template.resource_count_is("AWS::CodeCommit::Repository", 1)
    # 2 pipelines: one for dev, one for prod
    template.resource_count_is("AWS::CodePipeline::Pipeline", 2)
    # 3 CodeBuild projects: dev-build, prod-build, prod-deploy
    template.resource_count_is("AWS::CodeBuild::Project", 3)


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


def test_cicd_pipeline_has_dev_pipeline():
    app = cdk.App()
    stack = CicdPipelineStack(app, "TestCicdPipeline")
    template = assertions.Template.from_stack(stack)

    # Dev pipeline auto-deploys from development branch
    template.has_resource_properties(
        "AWS::CodePipeline::Pipeline",
        {
            "Name": "earthquake-etl-dev-pipeline",
        },
    )


def test_cicd_pipeline_has_prod_pipeline():
    app = cdk.App()
    stack = CicdPipelineStack(app, "TestCicdPipeline")
    template = assertions.Template.from_stack(stack)

    # Prod pipeline deploys from main branch with approval gate
    template.has_resource_properties(
        "AWS::CodePipeline::Pipeline",
        {
            "Name": "earthquake-etl-prod-pipeline",
        },
    )


def test_cicd_pipeline_has_codebuild_projects():
    app = cdk.App()
    stack = CicdPipelineStack(app, "TestCicdPipeline")
    template = assertions.Template.from_stack(stack)

    # DevBuildProject, ProdBuildProject, ProdDeployProject
    template.resource_count_is("AWS::CodeBuild::Project", 3)