from aws_cdk import Stack
from aws_cdk import aws_codecommit as codecommit
from aws_cdk import aws_codebuild as codebuild
from aws_cdk import aws_codepipeline as codepipeline
from aws_cdk import aws_codepipeline_actions as codepipeline_actions
from aws_cdk import pipelines
from constructs import Construct


class CicdPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        repo = codecommit.Repository(
            self,
            "ETLRepository",
            repository_name="earthquake-etl",
        )

        pipeline = codepipeline.Pipeline(
            self,
            "ETLPipeline",
            pipeline_name="earthquake-etl-pipeline",
        )

        source_output = codepipeline.Artifact()
        source_stage = pipeline.add_stage(stage_name="Source")
        source_action = codepipeline_actions.CodeCommitSourceAction(
            action_name="CodeCommitSource",
            repository=repo,
            branch="main",
            output=source_output,
        )
        source_stage.add_action(source_action)

        build_project = codebuild.PipelineProject(
            self,
            "CDKBuildProject",
            build_spec=codebuild.BuildSpec.from_object({
                "version": "0.2",
                "phases": {
                    "build": {
                        "commands": [
                            "npm install",
                            "cdk synth",
                            "cdk deploy --all --require-approval=never",
                        ]
                    }
                }
            }),
        )

        build_stage = pipeline.add_stage(stage_name="Build")
        build_action = codepipeline_actions.CodeBuildAction(
            action_name="CDKBuild",
            project=build_project,
            input=source_output,
        )
        build_stage.add_action(build_action)
