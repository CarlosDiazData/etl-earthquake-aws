from setuptools import setup, find_packages

setup(
    name="etl_earthquake_aws",
    version="1.0.0",
    description="AWS CDK infrastructure for Earthquake ETL pipeline",
    author="Your Name",
    packages=find_packages(where="."),
    package_dir={"": "."},
    install_requires=[
        "aws-cdk-lib>=2.170.0",
        "constructs>=10.0.0,<11.0.0",
    ],
    python_requires=">=3.9",
)
