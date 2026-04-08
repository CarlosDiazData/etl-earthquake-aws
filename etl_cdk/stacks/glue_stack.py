import pathlib
from aws_cdk import (
    Aws,
    Duration,
    Stack,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3_deployment as s3_deployment,
)
from constructs import Construct


class GlueStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        app_name: str,
        env_name: str,
        data_bucket,
        scripts_bucket,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

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
            f"s3://{scripts_bucket.bucket_name}/scripts/prosses_silver_gold.py"
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
