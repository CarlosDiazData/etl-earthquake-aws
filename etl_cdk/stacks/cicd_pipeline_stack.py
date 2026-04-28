from aws_cdk import Stack, Environment
from aws_cdk import aws_codecommit as codecommit
from aws_cdk import aws_codebuild as codebuild
from aws_cdk import aws_codepipeline as codepipeline
from aws_cdk import aws_codepipeline_actions as codepipeline_actions
from aws_cdk import aws_iam as iam
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

        self.repo = repo

        # =========================================================
        # PIPELINE 1: DEV - Auto-deploy from development branch
        # =========================================================
        dev_pipeline = codepipeline.Pipeline(
            self,
            "DevPipeline",
            pipeline_name="earthquake-etl-dev-pipeline",
        )

        # Source stage - listens to 'development' branch
        dev_source_output = codepipeline.Artifact()
        dev_source_stage = dev_pipeline.add_stage(stage_name="Source")

        dev_source_action = codepipeline_actions.CodeCommitSourceAction(
            action_name="CodeCommitSource_Dev",
            repository=repo,
            branch="development",
            output=dev_source_output,
        )
        dev_source_stage.add_action(dev_source_action)

        # Build stage
        dev_build_project = codebuild.PipelineProject(
            self,
            "DevBuildProject",
            build_spec=codebuild.BuildSpec.from_object({
                "version": "0.2",
                "phases": {
                    "install": {
                        "commands": [
                            "pip install -r requirements.txt",
                            "npm install",
                        ]
                    },
                    "build": {
                        "commands": [
                            "cdk synth --context ENV_NAME=dev",
                        ]
                    },
                    "post_build": {
                        "commands": [
                            "cdk deploy --all --context ENV_NAME=dev --require-approval=never",
                        ]
                    }
                }
            }),
        )

        dev_build_stage = dev_pipeline.add_stage(stage_name="Build")
        dev_build_action = codepipeline_actions.CodeBuildAction(
            action_name="CDKBuild_Dev",
            project=dev_build_project,
            input=dev_source_output,
        )
        dev_build_stage.add_action(dev_build_action)

        # =========================================================
        # PIPELINE 2: PROD - Deploy via PR to main with approval
        # =========================================================
        prod_pipeline = codepipeline.Pipeline(
            self,
            "ProdPipeline",
            pipeline_name="earthquake-etl-prod-pipeline",
        )

        # Source stage - listens to 'main' branch
        prod_source_output = codepipeline.Artifact()
        prod_source_stage = prod_pipeline.add_stage(stage_name="Source")

        prod_source_action = codepipeline_actions.CodeCommitSourceAction(
            action_name="CodeCommitSource_Prod",
            repository=repo,
            branch="main",
            output=prod_source_output,
        )
        prod_source_stage.add_action(prod_source_action)

        # Build stage - synth only, no deploy
        prod_build_project = codebuild.PipelineProject(
            self,
            "ProdBuildProject",
            build_spec=codebuild.BuildSpec.from_object({
                "version": "0.2",
                "phases": {
                    "install": {
                        "commands": [
                            "pip install -r requirements.txt",
                            "npm install",
                        ]
                    },
                    "build": {
                        "commands": [
                            "cdk synth --context ENV_NAME=prod",
                        ]
                    }
                },
                "artifacts": {
                    "secondary-artifacts": {
                        "stack-template": {
                            "base-directory": "cdk.out",
                            "files": ["*.template.json"],
                        }
                    }
                }
            }),
        )

        prod_build_stage = prod_pipeline.add_stage(stage_name="Build")
        prod_build_action = codepipeline_actions.CodeBuildAction(
            action_name="CDKBuild_Prod",
            project=prod_build_project,
            input=prod_source_output,
        )
        prod_build_stage.add_action(prod_build_action)

        # Manual Approval stage
        approval_stage = prod_pipeline.add_stage(stage_name="Approval")
        approval_action = codepipeline_actions.ManualApprovalAction(
            action_name="ProductionApproval",
        )
        approval_stage.add_action(approval_action)

        # Deploy to Production stage
        prod_deploy_project = codebuild.PipelineProject(
            self,
            "ProdDeployProject",
            build_spec=codebuild.BuildSpec.from_object({
                "version": "0.2",
                "phases": {
                    "build": {
                        "commands": [
                            "cdk deploy --all --context ENV_NAME=prod --require-approval=never",
                        ]
                    }
                }
            }),
        )

        prod_deploy_stage = prod_pipeline.add_stage(stage_name="Deploy-Prod")
        prod_deploy_action = codepipeline_actions.CodeBuildAction(
            action_name="CDKDeploy_Prod",
            project=prod_deploy_project,
            input=prod_source_output,
        )
        prod_deploy_stage.add_action(prod_deploy_action)

        # =========================================================
        # Outputs
        # =========================================================
        self.dev_pipeline = dev_pipeline
        self.prod_pipeline = prod_pipeline