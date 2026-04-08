from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    Tags,
)
from aws_cdk import aws_s3 as s3
from constructs import Construct


class DataLakeStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        app_name: str,
        env_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.data_bucket = s3.Bucket(
            self,
            "EarthquakeDataBucket",
            bucket_name=f"{app_name}-data-{env_name}-{self.account}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="ArchiveOldBronzeData",
                    prefix="bronze/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(30),
                        ),
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(90),
                        ),
                    ],
                    expiration=Duration.days(365),
                ),
                s3.LifecycleRule(
                    id="ArchiveOldSilverData",
                    prefix="silver/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(180),
                        ),
                    ],
                    expiration=Duration.days(730),
                ),
                s3.LifecycleRule(
                    id="ArchiveOldGoldData",
                    prefix="gold/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(365),
                        ),
                    ],
                    expiration=Duration.days(1825),
                ),
            ],
            server_access_logs_prefix="data-access-logs/",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        self.scripts_bucket = s3.Bucket(
            self,
            "ScriptsBucket",
            bucket_name=f"{app_name}-scripts-{env_name}-{self.account}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="ExpireOldScripts",
                    prefix="scripts/",
                    noncurrent_version_expiration=Duration.days(30),
                ),
            ],
            server_access_logs_prefix="scripts-access-logs/",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        Tags.of(self).add("Project", app_name)
        Tags.of(self).add("Environment", env_name)
        Tags.of(self).add("DataClassification", "Public")
