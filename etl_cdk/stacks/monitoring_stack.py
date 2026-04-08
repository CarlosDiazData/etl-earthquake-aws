from aws_cdk import (
    Duration,
    Stack,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as actions,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
)
from constructs import Construct


class MonitoringStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        app_name: str,
        env_name: str,
        data_bucket,
        bronze_to_silver_job,
        silver_to_gold_job,
        state_machine,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        alert_topic = sns.Topic(
            self,
            "ETLAlerts",
            topic_name=f"{app_name}-etl-alerts-{env_name}",
            display_name="Earthquake ETL Pipeline Alerts",
        )

        alert_topic.add_subscription(
            subs.EmailSubscription("admin@example.com")
        )

        bronze_to_silver_failure_alarm = cloudwatch.Alarm(
            self,
            "BronzeToSilverFailureAlarm",
            alarm_name=f"{app_name}-bronze-to-silver-failure-{env_name}",
            metric=cloudwatch.Metric(
                namespace="AWS/Glue",
                metric_name="glue.driver.aggregate.numFailedJobs",
                dimensions_map={"JobName": bronze_to_silver_job.name},
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
                dimensions_map={"JobName": silver_to_gold_job.name},
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
            metric=state_machine.metric_failed(
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
            metric=state_machine.metric_time(
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
                        dimensions_map={"JobName": bronze_to_silver_job.name},
                        statistic="Sum",
                        period=Duration.hours(1),
                    ),
                    cloudwatch.Metric(
                        namespace="AWS/Glue",
                        metric_name="glue.driver.aggregate.numFailedJobs",
                        dimensions_map={"JobName": bronze_to_silver_job.name},
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
                        dimensions_map={"JobName": silver_to_gold_job.name},
                        statistic="Sum",
                        period=Duration.hours(1),
                    ),
                    cloudwatch.Metric(
                        namespace="AWS/Glue",
                        metric_name="glue.driver.aggregate.numFailedJobs",
                        dimensions_map={"JobName": silver_to_gold_job.name},
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
                    state_machine.metric_started(statistic="Sum", period=Duration.hours(1)),
                    state_machine.metric_succeeded(statistic="Sum", period=Duration.hours(1)),
                    state_machine.metric_failed(statistic="Sum", period=Duration.hours(1)),
                ],
            ),
            cloudwatch.GraphWidget(
                title="ETL Pipeline Duration",
                left=[
                    state_machine.metric_time(
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
