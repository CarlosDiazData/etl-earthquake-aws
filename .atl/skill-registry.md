# Skill Registry

**Project**: etl-earthquake-aws
**Generated**: 2026-04-28

## Available Skills

| Skill | Source | Trigger Context |
|-------|--------|-----------------|
| sdd-init | user (~/.config/opencode/skills/) | Initialize SDD in project |
| sdd-explore | user (~/.config/opencode/skills/) | Explore ideas before committing to change |
| sdd-propose | user (~/.config/opencode/skills/) | Create change proposal |
| sdd-spec | user (~/.config/opencode/skills/) | Write specifications |
| sdd-design | user (~/.config/opencode/skills/) | Create technical design |
| sdd-tasks | user (~/.config/opencode/skills/) | Break down into tasks |
| sdd-apply | user (~/.config/opencode/skills/) | Implement tasks |
| sdd-verify | user (~/.config/opencode/skills/) | Validate implementation |
| sdd-archive | user (~/.config/opencode/skills/) | Archive completed change |
| sdd-onboard | user (~/.config/opencode/skills/) | Guided SDD walkthrough |
| aws-cdk-development | user (~/.config/opencode/skills/) | AWS CDK with TypeScript/Python |
| aws-serverless-eda | user (~/.config/opencode/skills/) | Serverless & event-driven architecture |
| aws-cost-operations | user (~/.config/opencode/skills/) | AWS cost optimization & monitoring |
| skill-registry | user (~/.config/opencode/skills/) | Update skill registry |
| skill-creator | user (~/.config/opencode/skills/) | Create new skills |

## Project Conventions

- **Stack**: AWS CDK Python (cdk.json configured)
- **Testing**: pytest with aws-cdk.assertions
- **Architecture**: ETL pipeline with S3, Lambda, Glue, Step Functions, CloudWatch
- **No linter/formatter configured** - consider adding ruff for linting/formatting