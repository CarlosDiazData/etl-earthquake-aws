import pathlib
from aws_cdk import (
    Aws,
    Duration,
    RemovalPolicy,
    Stack,
    CfnParameter,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3_deployment as s3_deployment,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_logs as logs,
    aws_events as events,
    aws_events_targets as targets,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as actions,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
)
from aws_cdk.aws_s3 import IBucket
from aws_cdk.aws_lambda import IFunction
from constructs import Construct


class PipelineStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        app_name: str,
        env_name: str,
        data_bucket: IBucket,
        scripts_bucket: IBucket,
        ingestion_lambda: IFunction,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # =============================================================================
        # Section: Glue Resources
        # =============================================================================

        scripts_path = str(
            pathlib.Path(__file__).resolve().parent.parent.parent / "scripts"
        )

        s3_deployment.BucketDeployment(
            self,
            "DeployScripts",
            sources=[s3_deployment.Source.asset(scripts_path)],
            destination_bucket=scripts_bucket,
            destination_key_prefix="scripts",
        )

        self.gold_database = glue.CfnDatabase(
            self,
            "GoldDatabase",
            catalog_id=Aws.ACCOUNT_ID,
            database_name="gold_earthquakes",
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                description="Gold layer database for earthquake dimensional model",
            ),
        )

        glue_service_role = iam.Role(
            self,
            "GlueServiceRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description="Service role for AWS Glue",
            max_session_duration=Duration.hours(12),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        glue_service_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                ],
                resources=[
                    data_bucket.bucket_arn,
                    f"{data_bucket.bucket_arn}/*",
                    scripts_bucket.bucket_arn,
                    f"{scripts_bucket.bucket_arn}/*",
                ],
            )
        )

        glue_service_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "kms:Encrypt",
                    "kms:Decrypt",
                    "kms:ReEncrypt*",
                    "kms:GenerateDataKey*",
                    "kms:DescribeKey",
                ],
                resources=["*"],
            )
        )

        glue_service_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:CreateDatabase",
                    "glue:UpdateDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:BatchCreatePartition",
                    "glue:CreatePartition",
                    "glue:UpdatePartition",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:BatchGetPartition",
                ],
                resources=[
                    f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog",
                    f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/default",
                    f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{self.gold_database.database_input.name}",
                    f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/{self.gold_database.database_input.name}/*",
                ],
            )
        )

        glue_service_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=[
                    f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws-glue/*"
                ],
            )
        )

        bronze_to_silver_script_location = (
            f"s3://{scripts_bucket.bucket_name}/scripts/process_bronze_to_silver.py"
        )
        silver_to_gold_script_location = (
            f"s3://{scripts_bucket.bucket_name}/scripts/process_silver_to_gold.py"
        )

        self.bronze_to_silver_job = glue.CfnJob(
            self,
            "BronzeToSilverJob",
            name=f"{app_name}-bronze-to-silver-{env_name}",
            role=glue_service_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=bronze_to_silver_script_location,
                python_version="3",
            ),
            default_arguments={
                "--job-language": "python",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": f"s3://{data_bucket.bucket_name}/spark-logs/",
                "--TempDir": f"s3://{data_bucket.bucket_name}/temp/",
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-xray-tracing": "true",
                "--S3_BUCKET_NAME": data_bucket.bucket_name,
                "--conf": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
            },
            glue_version="5.0",
            number_of_workers=5,
            worker_type="G.2X",
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            ),
        )

        self.silver_to_gold_job = glue.CfnJob(
            self,
            "SilverToGoldJob",
            name=f"{app_name}-silver-to-gold-{env_name}",
            role=glue_service_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=silver_to_gold_script_location,
                python_version="3",
            ),
            default_arguments={
                "--job-language": "python",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": f"s3://{data_bucket.bucket_name}/spark-logs/",
                "--TempDir": f"s3://{data_bucket.bucket_name}/temp/",
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-xray-tracing": "true",
                "--S3_BUCKET_NAME": data_bucket.bucket_name,
                "--conf": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
            },
            glue_version="5.0",
            number_of_workers=5,
            worker_type="G.2X",
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            ),
        )

        self.bronze_to_silver_job.add_dependency(self.gold_database)
        self.silver_to_gold_job.add_dependency(self.gold_database)

        # =============================================================================
        # Section: Step Functions Resources
        # =============================================================================

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
            glue_job_name=self.bronze_to_silver_job.name,
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
            glue_job_name=self.silver_to_gold_job.name,
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
                .when(
                    sfn.Condition.number_equals("$.recordsCount", 0),
                    sfn.Pass(self, "NoRecordsSkipped"),
                )
                .when(
                    sfn.Condition.number_greater_than("$.recordsCount", 0),
                    bronze_to_silver_task.next(silver_to_gold_task),
                )
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

        # =============================================================================
        # Section: Monitoring Resources
        # =============================================================================

        alert_topic = sns.Topic(
            self,
            "ETLAlerts",
            topic_name=f"{app_name}-etl-alerts-{env_name}",
            display_name="Earthquake ETL Pipeline Alerts",
        )

        alert_email = CfnParameter(
            self,
            "AlertEmail",
            type="String",
            default="admin@example.com",
            description="Email address for ETL alert notifications",
            allowed_pattern=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
            no_echo=False,
        )

        alert_topic.add_subscription(
            subs.EmailSubscription(alert_email.value_as_string)
        )

        bronze_to_silver_failure_alarm = cloudwatch.Alarm(
            self,
            "BronzeToSilverFailureAlarm",
            alarm_name=f"{app_name}-bronze-to-silver-failure-{env_name}",
            metric=cloudwatch.Metric(
                namespace="AWS/Glue",
                metric_name="glue.driver.aggregate.numFailedJobs",
                dimensions_map={"JobName": self.bronze_to_silver_job.name},
                statistic="Sum",
                period=Duration.minutes(5),
            ),
            threshold=1,
            evaluation_periods=1,
            alarm_description="Bronze to Silver Glue job has failed",
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        bronze_to_silver_failure_alarm.add_alarm_action(actions.SnsAction(alert_topic))

        silver_to_gold_failure_alarm = cloudwatch.Alarm(
            self,
            "SilverToGoldFailureAlarm",
            alarm_name=f"{app_name}-silver-to-gold-failure-{env_name}",
            metric=cloudwatch.Metric(
                namespace="AWS/Glue",
                metric_name="glue.driver.aggregate.numFailedJobs",
                dimensions_map={"JobName": self.silver_to_gold_job.name},
                statistic="Sum",
                period=Duration.minutes(5),
            ),
            threshold=1,
            evaluation_periods=1,
            alarm_description="Silver to Gold Glue job has failed",
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        silver_to_gold_failure_alarm.add_alarm_action(actions.SnsAction(alert_topic))

        state_machine_failure_alarm = cloudwatch.Alarm(
            self,
            "StateMachineFailureAlarm",
            alarm_name=f"{app_name}-state-machine-failure-{env_name}",
            metric=self.state_machine.metric_failed(
                period=Duration.minutes(5),
            ),
            threshold=1,
            evaluation_periods=1,
            alarm_description="ETL Step Functions workflow has failed",
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        state_machine_failure_alarm.add_alarm_action(actions.SnsAction(alert_topic))

        state_machine_duration_alarm = cloudwatch.Alarm(
            self,
            "StateMachineDurationAlarm",
            alarm_name=f"{app_name}-state-machine-duration-{env_name}",
            metric=self.state_machine.metric_time(
                period=Duration.minutes(5),
            ),
            threshold=10800,
            evaluation_periods=1,
            alarm_description="ETL pipeline is taking longer than 3 hours",
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        state_machine_duration_alarm.add_alarm_action(actions.SnsAction(alert_topic))

        dashboard = cloudwatch.Dashboard(
            self,
            "ETLDashboard",
            dashboard_name=f"{app_name}-etl-dashboard-{env_name}",
        )

        dashboard.add_widgets(
            cloudwatch.GraphWidget(
                title="Glue Job Runs - Bronze to Silver",
                left=[
                    cloudwatch.Metric(
                        namespace="AWS/Glue",
                        metric_name="glue.driver.aggregate.numCompletedJobs",
                        dimensions_map={"JobName": self.bronze_to_silver_job.name},
                        statistic="Sum",
                        period=Duration.hours(1),
                    ),
                    cloudwatch.Metric(
                        namespace="AWS/Glue",
                        metric_name="glue.driver.aggregate.numFailedJobs",
                        dimensions_map={"JobName": self.bronze_to_silver_job.name},
                        statistic="Sum",
                        period=Duration.hours(1),
                        color=cloudwatch.Color.RED,
                    ),
                ],
            ),
            cloudwatch.GraphWidget(
                title="Glue Job Runs - Silver to Gold",
                left=[
                    cloudwatch.Metric(
                        namespace="AWS/Glue",
                        metric_name="glue.driver.aggregate.numCompletedJobs",
                        dimensions_map={"JobName": self.silver_to_gold_job.name},
                        statistic="Sum",
                        period=Duration.hours(1),
                    ),
                    cloudwatch.Metric(
                        namespace="AWS/Glue",
                        metric_name="glue.driver.aggregate.numFailedJobs",
                        dimensions_map={"JobName": self.silver_to_gold_job.name},
                        statistic="Sum",
                        period=Duration.hours(1),
                        color=cloudwatch.Color.RED,
                    ),
                ],
            ),
        )

        dashboard.add_widgets(
            cloudwatch.GraphWidget(
                title="Step Functions Executions",
                left=[
                    self.state_machine.metric_started(
                        statistic="Sum", period=Duration.hours(1)
                    ),
                    self.state_machine.metric_succeeded(
                        statistic="Sum", period=Duration.hours(1)
                    ),
                    self.state_machine.metric_failed(
                        statistic="Sum", period=Duration.hours(1)
                    ),
                ],
            ),
            cloudwatch.GraphWidget(
                title="ETL Pipeline Duration",
                left=[
                    self.state_machine.metric_time(
                        statistic="Average",
                        period=Duration.hours(1),
                    ),
                ],
            ),
        )

        dashboard.add_widgets(
            cloudwatch.GraphWidget(
                title="S3 Data Volume",
                left=[
                    cloudwatch.Metric(
                        namespace="AWS/S3",
                        metric_name="NumberOfObjects",
                        dimensions_map={"BucketName": data_bucket.bucket_name},
                        statistic="Average",
                        period=Duration.hours(1),
                    ),
                ],
            ),
        )

        # =============================================================================
        # Section: EventBridge Schedule
        # =============================================================================

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
