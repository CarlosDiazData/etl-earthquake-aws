from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_logs as logs,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
)
from constructs import Construct


class OrchestrationStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        app_name: str,
        env_name: str,
        ingestion_lambda,
        bronze_to_silver_job,
        silver_to_gold_job,
        data_bucket,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        state_machine_log_group = logs.LogGroup(
            self,
            "ETLStateMachineLogs",
            log_group_name=f"/aws/stepfunctions/{app_name}-etl-pipeline-{env_name}",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        invoke_lambda = tasks.LambdaInvoke(
            self,
            "IngestEarthquakeData",
            lambda_function=ingestion_lambda,
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=True,
            result_selector={
                "recordsCount.$": "$.Payload.recordsCount",
            },
        )

        invoke_lambda.add_retry(
            errors=[
                "Lambda.TooManyRequestsException",
                "Lambda.ServiceException",
            ],
            interval=Duration.minutes(1),
            max_attempts=3,
            backoff_rate=2,
        )

        # Catch Lambda failures and set recordsCount=0 so the Choice state has a valid path
        invoke_lambda.add_catch(
            handler=sfn.Pass(
                self,
                "LambdaFailed",
                parameters={
                    "recordsCount": 0,
                    "error": "lambdaError",
                },
            ),
            errors=["States.ALL"],
        )

        bronze_to_silver_task = tasks.GlueStartJobRun(
            self,
            "ProcessBronzeToSilver",
            glue_job_name=bronze_to_silver_job.name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object(
                {
                    "--S3_BUCKET_NAME": data_bucket.bucket_name,
                }
            ),
        )

        bronze_to_silver_task.add_retry(
            errors=[
                "Glue.AWSGlueException",
                "Glue.InternalServiceException",
                "States.Timeout",
            ],
            interval=Duration.minutes(2),
            max_attempts=3,
            backoff_rate=2,
            jitter_strategy=sfn.JitterType.FULL,
        )

        silver_to_gold_task = tasks.GlueStartJobRun(
            self,
            "ProcessSilverToGold",
            glue_job_name=silver_to_gold_job.name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object(
                {
                    "--S3_BUCKET_NAME": data_bucket.bucket_name,
                }
            ),
        )

        silver_to_gold_task.add_retry(
            errors=[
                "Glue.AWSGlueException",
                "Glue.InternalServiceException",
                "States.Timeout",
            ],
            interval=Duration.minutes(2),
            max_attempts=3,
            backoff_rate=2,
            jitter_strategy=sfn.JitterType.FULL,
        )

        definition = (
            invoke_lambda.next(
                sfn.Choice(self, "CheckRecordsCount")
                .when(sfn.Condition.number_equals("$.recordsCount", 0), sfn.Pass(self, "NoRecordsSkipped"))
                .when(sfn.Condition.number_greater_than("$.recordsCount", 0), bronze_to_silver_task.next(silver_to_gold_task))
                .otherwise(sfn.Pass(self, "UnexpectedState"))
            )
        )

        self.state_machine = sfn.StateMachine(
            self,
            "ETLStateMachine",
            state_machine_name=f"{app_name}-etl-pipeline-{env_name}",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(4),
            tracing_enabled=True,
            logs=sfn.LogOptions(
                destination=state_machine_log_group,
                level=sfn.LogLevel.ALL,
            ),
        )

        schedule_rule = events.Rule(
            self,
            "ETLPipelineSchedule",
            rule_name=f"{app_name}-etl-pipeline-schedule-{env_name}",
            description="Triggers the full ETL pipeline every 6 hours",
            schedule=events.Schedule.cron(
                minute="0",
                hour="0/6",
            ),
        )

        schedule_rule.add_target(targets.SfnStateMachine(self.state_machine))
