import pathlib
from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
)
from constructs import Construct


class IngestionStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        app_name: str,
        env_name: str,
        data_bucket,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        lambda_role = iam.Role(
            self,
            "IngestionLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Role for earthquake data ingestion Lambda",
            max_session_duration=Duration.hours(1),
        )

        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicExecutionRole"
            )
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                ],
                resources=[
                    f"{data_bucket.bucket_arn}/bronze/*",
                ],
            )
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "xray:PutTraceSegments",
                    "xray:GetTraceGraph",
                ],
                resources=["*"],
            )
        )

        script_path = str(
            pathlib.Path(__file__).resolve().parent.parent.parent / "lambda_code"
        )

        self.ingestion_lambda = _lambda.Function(
            self,
            "EarthquakeIngestionLambda",
            function_name=f"{app_name}-ingestion-{env_name}",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="api_to_bronze.lambda_handler",
            code=_lambda.Code.from_asset(script_path),
            role=lambda_role,
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "S3_BUCKET_NAME": data_bucket.bucket_name,
            },
            description="Fetches earthquake data from USGS API and stores in S3 Bronze layer",
            tracing=_lambda.Tracing.ACTIVE,
        )

        schedule_rule = events.Rule(
            self,
            "IngestionSchedule",
            rule_name=f"{app_name}-ingestion-schedule-{env_name}",
            description="Triggers earthquake data ingestion every 6 hours",
            schedule=events.Schedule.cron(
                minute="0",
                hour="0/6",
            ),
        )

        schedule_rule.add_target(targets.LambdaFunction(self.ingestion_lambda))
