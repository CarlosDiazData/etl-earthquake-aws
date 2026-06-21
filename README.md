# Earthquake ETL Pipeline

[![AWS](https://img.shields.io/badge/AWS-CDK_2.248.0-orange)](https://aws.amazon.com/cdk/)
[![Python](https://img.shields.io/badge/Python-3.12-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

A production-ready ETL (Extract, Transform, Load) data pipeline on AWS that ingests earthquake data from the USGS (United States Geological Survey) API, processes it through a medallion architecture (Bronze/Silver/Gold), and provides monitoring and alerting capabilities.

## Dashboard

![](https://raw.githubusercontent.com/CarlosDiazData/etl-earthquake-gcp/refs/heads/main/reports/assets/report%20overview.png)

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              EARTHQUAKE ETL PIPELINE                               │
└─────────────────────────────────────────────────────────────────────────────────────┘

    ┌─────────────┐     ┌─────────────────────────────────────────────────────────┐
    │   USGS API  │────▶│                    INGESTION LAYER                     │
    │ (Earthquake │     │  ┌─────────────────────────────────────────────────────┐  │
    │    Data)    │     │  │  AWS Lambda (Scheduled via EventBridge)            │  │
    └─────────────┘     │  │  - Fetches data from USGS REST API                   │  │
                      │  │  - Stores raw JSON to S3 Bronze Layer                 │  │
                      │  └─────────────────────────────────────────────────────┘  │
                      └──────────────────────────┬────────────────────────────────┘
                                                 │
                                                 ▼
    ┌───────────────────────────────────────────────────────────────────────────────┐
    │                           DATA LAKE (S3)                                       │
    │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────────────┐  │
    │  │     BRONZE       │  │      SILVER      │  │           GOLD               │  │
    │  │   (Raw Data)    │──▶│  (Cleaned Data)  │──▶│    (Aggregated/Analytics)   │  │
    │  │                  │  │                  │  │                              │  │
    │  │ - Raw JSON       │  │ - Delta Format   │  │ - Parquet Format            │  │
    │  │ - Immutable      │  │ - Deduplicated   │  │ - Business-ready tables     │  │
    │  │ - Versioned     │  │ - Validated      │  │ - Aggregations              │  │
    │  └──────────────────┘  └──────────────────┘  └──────────────────────────────┘  │
    └───────────────────────────────────────────────────────────────────────────────┘
                                                 │
    ┌────────────────────────────────────────────┴───────────────────────────────────┐
    │                           PROCESSING LAYER                                     │
    │  ┌─────────────────────────────────────────────────────────────────────────────┐│
    │  │                    AWS GLUE JOBS (PySpark)                                 ││
    │  │  ┌─────────────────────────────┐  ┌─────────────────────────────────────────┐││
    │  │  │   Bronze → Silver Job      │  │   Silver → Gold Job                    │││
    │  │  │   - Flatten JSON           │  │   - Aggregations                      │││
    │  │  │   - Data validation         │  │   - Business transformations          │││
    │  │  │   - Deduplication           │  │   - Analytics tables                  │││
    │  │  │   - Feature engineering     │  │                                       │││
    │  │  └─────────────────────────────┘  └─────────────────────────────────────────┘││
    │  └─────────────────────────────────────────────────────────────────────────────┘│
    └─────────────────────────────────────────────────────────────────────────────────┘
                                                 │
    ┌────────────────────────────────────────────┴───────────────────────────────────┐
    │                          ORCHESTRATION LAYER                                   │
    │  ┌─────────────────────────────────────────────────────────────────────────────┐│
    │  │                    AWS STEP FUNCTIONS                                      ││
    │  │                                                                          ││
    │  │   ┌──────────┐    ┌─────────────────┐    ┌────────────────────────────┐   ││
    │  │   │ Ingestion│───▶│Bronze→Silver Job│───▶│Silver→Gold Job             │   ││
    │  │   │ Lambda   │    │ (Glue)          │    │ (Glue)                     │   ││
    │  │   └──────────┘    └─────────────────┘    └────────────────────────────┘   ││
    │  │                                                                          ││
    │  │   ┌─────────────────────────────────────────────────────────────────┐     ││
    │  │   │                    Error Handling & Retry Logic               │     ││
    │  │   └─────────────────────────────────────────────────────────────────┘     ││
    │  └─────────────────────────────────────────────────────────────────────────────┘│
    └─────────────────────────────────────────────────────────────────────────────────┘
                                                 │
    ┌────────────────────────────────────────────┴───────────────────────────────────┐
    │                           MONITORING LAYER                                     │
    │  ┌─────────────────────────────────────────────────────────────────────────────┐│
    │  │                    CLOUDWATCH                                             ││
    │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  ││
    │  │  │Job Failures │  │Duration     │  │Cost Alarms  │  │   Dashboard    │  ││
    │  │  │   Alarm     │  │   Alarm     │  │   Alarm     │  │   (Metrics)    │  ││
    │  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────┘  ││
    │  │                                                                          ││
    │  │  ┌─────────────────────────────────────────────────────────────────────┐   ││
    │  │  │ SNS Topic → Email Notifications                                    │   ││
    │  │  └─────────────────────────────────────────────────────────────────────┘   ││
    │  └─────────────────────────────────────────────────────────────────────────────┘│
    └─────────────────────────────────────────────────────────────────────────────────┘
```

## Technology Stack

| Category | Technology | Version |
|----------|------------|---------|
| **IaC** | AWS CDK | 2.248.0 |
| **Language** | Python | 3.12 |
| **Data Processing** | AWS Glue (PySpark) | 5.0 |
| **Serverless** | AWS Lambda | Python 3.12 |
| **Orchestration** | AWS Step Functions | - |
| **Scheduling** | Amazon EventBridge | - |
| **Storage** | Amazon S3 | - |
| **Monitoring** | Amazon CloudWatch | - |
| **Testing** | pytest, CDK Assertions | - |

## Project Structure

```
etl-earthquake-aws/
├── app.py                     # CDK Application entry point
├── cdk.json                   # CDK configuration
├── requirements.txt           # Production dependencies
├── setup.py                   # Package setup
│
├── etl_cdk/                   # CDK Infrastructure Code
│   ├── stacks/
│   │   ├── data_lake_stack.py      # S3 buckets (Bronze/Silver/Gold)
│   │   ├── ingestion_stack.py      # Lambda ingestion layer
│   │   ├── glue_stack.py           # Glue jobs (Bronze→Silver, Silver→Gold)
│   │   ├── orchestration_stack.py  # Step Functions state machine
│   │   └── monitoring_stack.py    # CloudWatch alarms & dashboard
│   ├── constructs/             # Reusable CDK constructs
│   └── config/                 # Configuration modules
│
├── lambda_code/
│   └── api_to_bronze.py       # USGS API ingestion Lambda
│
├── scripts/
│   ├── process_bronze_to_silver.py   # Bronze→Silver Glue job
│   └── process_silver_to_gold.py        # Silver→Gold Glue job
│
└── tests/
    └── unit/
        └── test_etl_stack.py  # CDK unit tests
```

## Getting Started

### Prerequisites

- **AWS Account** with appropriate permissions
- **AWS CLI** configured with credentials
- **Python 3.12** or higher
- **Node.js** (for CDK)
- **CDK Bootstrap** deployed to your account

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd etl-earthquake-aws
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   # source .venv/bin/activate  # Linux/Mac
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set environment variables**
   ```bash
   export CDK_DEFAULT_ACCOUNT=<your-aws-account-id>
   export CDK_DEFAULT_REGION=<desired-region>
   export ENV_NAME=dev
   ```

### Deploying the Stack

```bash
# Synthesize the CloudFormation template
cdk synth

# Deploy the stack
cdk deploy --all
```

### Running Tests

```bash
# Install test dependencies
pip install -r requirements-dev.txt

# Run unit tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ -v --cov=etl_cdk
```

## Key Features

### 1. Data Ingestion
- **Scheduled Lambda** fetches earthquake data from USGS REST API
- Configurable time windows (default: past 365 days)
- Minimum magnitude filtering (default: 2.5)
- Automatic storage to S3 Bronze layer

### 2. Data Processing (Bronze → Silver)
- **Flattening**: Extracts nested GeoJSON to tabular format
- **Data Validation**: Validates coordinates, timestamps, magnitude ranges
- **Deduplication**: Keeps latest version per event ID
- **Feature Engineering**:
  - Magnitude categories (Micro, Minor, Light, Moderate, Strong, Major, Great)
  - Depth categories (Shallow, Intermediate, Deep)
  - Temporal fields (year, month, day, hour, day_of_week)
  - Location extraction (country, region)

### 3. Data Aggregation (Silver → Gold)
- Aggregations by region, magnitude, time period
- Business-ready analytical tables in Parquet format

### 4. Orchestration
- **Step Functions** manages the complete ETL workflow
- Automatic retry logic for transient failures
- Comprehensive logging and tracing

### 5. Monitoring & Alerting
- **CloudWatch Dashboard** with real-time metrics
- **Alarms** for:
  - Glue job failures
  - Step Functions failures
  - Pipeline duration exceeding SLA
  - Monthly cost budget alerts
- **SNS notifications** for all alerts

## Development Workflow

### Branching Strategy
- `main` - Production-ready code
- `develop` - Integration branch
- Feature branches - `feature/<feature-name>`

### Code Quality
- Follows AWS CDK best practices
- Type hints for all Python code
- Comprehensive unit tests for CDK stacks

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `CDK_DEFAULT_ACCOUNT` | AWS Account ID | Required |
| `CDK_DEFAULT_REGION` | AWS Region | `us-east-1` |
| `ENV_NAME` | Environment name | `dev` |

### Tuning Parameters

**Lambda Ingestion** (`lambda_code/api_to_bronze.py`):
- `USGS_API_BASE_URL`: USGS API endpoint
- `minmagnitude`: Minimum earthquake magnitude (default: 2.5)
- `limit`: Maximum records per fetch (default: 20000)

**Glue Jobs** (`scripts/process_bronze_to_silver.py`):
- Worker type: G.2X (configurable)
- Number of workers: 5 (configurable)
- Glue Version: 5.0

## Security

- **S3 Buckets**: Versioning enabled, encryption (SSE-S3), public access blocked
- **IAM Roles**: Least privilege access policies
- **Network**: All resources in private subnets
- **Data Classification**: Tagged as "Public" for earthquake data

## Cost Optimization

- **S3 Lifecycle Policies**: Automatically archive cold data to Glacier
- **Glue Flex**: Use flexible instances for non-urgent jobs
- **Partitioning**: Data partitioned by year/month for efficient queries

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## References

- [AWS CDK Documentation](https://docs.aws.amazon.com/cdk/)
- [USGS Earthquake API](https://earthquake.usgs.gov/fdsnws/event/1/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Delta Lake Documentation](https://docs.delta.io/)
# Trigger workflow
