## Exploration: CDK Stacks del Pipeline ETL de Terremotos

### Estado Actual

El proyecto `etl-earthquake-aws` implementa un pipeline ETL serverless en AWS que obtiene datos sísmicos de la API pública de USGS, los almacena en un data lake con arquitectura de capas (Bronce → Plata → Oro), y expone los resultados para análisis. La infraestructura se define con AWS CDK (Python) y se despliega mediante GitHub Actions.

Existen **5 stacks CDK**, cada uno con una responsabilidad bien definida dentro del pipeline.

### Árbol de Dependencias

```
DataLakeStack  (base — sin dependencias)
  ├── IngestionStack     (depende de DataLakeStack)
  ├── GlueStack          (depende de DataLakeStack)
  └── OrchestrationStack (depende de IngestionStack + GlueStack)
       └── MonitoringStack  (depende de todos los anteriores, sin dep explícita)
```

### 1. DataLakeStack

**Archivo**: `etl_cdk/stacks/data_lake_stack.py`
**ID**: `earthquake-etl-data-lake-{env}`
**Dependencias**: Ninguna (es la base)

**Qué crea**:
- **KMS Key** (`DataLakeKmsKey`): Clave de cifrado rotada automáticamente para toda la capa de almacenamiento. Tiene una policy que permite a S3 usar la clave solo para los buckets propios del proyecto.
- **Data Bucket** (`EarthquakeDataBucket`): `{app}-data-{env}-{account}` — el bucket principal con tres prefijos que representan las capas del data lake:
  - `bronze/`: datos crudos → pasa a Intelligent Tiering a los 30 días, Glacier a los 90, expira al año
  - `silver/`: datos limpios → Intelligent Tiering a los 180 días, expira a los 2 años
  - `gold/`: datos modelados → Intelligent Tiering al año, expira a los 5 años
- **Scripts Bucket** (`ScriptsBucket`): `{app}-scripts-{env}-{account}` — almacena los scripts de Glue (ETL) con versionado y expiración de versiones viejas a los 30 días.

**Tags**: `Project: earthquake-etl`, `Environment: {env}`, `DataClassification: Public`

**Conexiones**: Expone `data_bucket` y `scripts_bucket` como atributos. Ambos buckets se pasan a los stacks que los necesitan.

---

### 2. IngestionStack

**Archivo**: `etl_cdk/stacks/ingestion_stack.py`
**ID**: `earthquake-etl-ingestion-{env}`
**Dependencias**: `DataLakeStack` (explícita via `add_dependency`)

**Qué crea**:
- **IAM Role** (`IngestionLambdaRole`): Rol para la Lambda con `AWSLambdaBasicExecutionRole` + permisos para escribir en `s3://bucket/bronze/*` + permisos de X-Ray.
- **Lambda Function** (`EarthquakeIngestionLambda`): `{app}-ingestion-{env}` — runtime Python 3.12, 512 MB, timeout 5 min, trazabilidad X-Ray activada.
  - **Handler**: `api_to_bronze.lambda_handler` (código en `lambda_code/`)
  - **Variable de entorno**: `S3_BUCKET_NAME`
  - **Propósito**: Obtener datos de la API de USGS (terremotos) y escribir los archivos JSON crudos en la capa `bronze/` del data lake.
- **EventBridge Rule** (`IngestionSchedule`): `{app}-ingestion-schedule-{env}` — dispara la Lambda **cada 6 horas** (cron: `0 0/6 * * ? *`).

**Conexiones**: Recibe `data_bucket` de DataLakeStack. Expone `ingestion_lambda` para que OrchestrationStack pueda invocarla desde Step Functions.

---

### 3. GlueStack

**Archivo**: `etl_cdk/stacks/glue_stack.py`
**ID**: `earthquake-etl-glue-{env}`
**Dependencias**: `DataLakeStack` (explícita)

**Qué crea**:
- **BucketDeployment**: Sube los scripts locales (`scripts/`) al `scripts_bucket` bajo el prefijo `scripts/`.
- **Glue Database** (`GoldDatabase`): `gold_earthquakes` — base de datos del Catálogo de Glue para la capa Gold (modelo dimensional).
- **IAM Role** (`GlueServiceRole`): Rol para Glue con permisos de S3 (leer/escribir en ambos buckets), KMS (cifrado), Glue Catalog (gestión de tablas/particiones), y CloudWatch Logs.
- **Glue Job #1** (`BronzeToSilverJob`): `{app}-bronze-to-silver-{env}` — script `process_bronze_to_silver.py`.
  - Glue 5.0, 5 workers G.2X, Spark con soporte Delta Lake
  - Bookmark activado, X-Ray, métricas, Spark UI
- **Glue Job #2** (`SilverToGoldJob`): `{app}-silver-to-gold-{env}` — script `prosses_silver_gold.py` (nota: typo en el nombre del archivo `prosses` vs `process`).
  - Misma configuración que BronzeToSilver

**Conexiones**: Recibe `data_bucket` y `scripts_bucket` de DataLakeStack. Expone `bronze_to_silver_job` y `silver_to_gold_job` para OrchestrationStack y MonitoringStack.

---

### 4. OrchestrationStack

**Archivo**: `etl_cdk/stacks/orchestration_stack.py`
**ID**: `earthquake-etl-orchestration-{env}`
**Dependencias**: `IngestionStack` + `GlueStack` (explícitas)

**Qué crea**:
- **CloudWatch Log Group** (`ETLStateMachineLogs`): `/aws/stepfunctions/{app}-etl-pipeline-{env}` — retención 1 mes.
- **Step Functions State Machine** (`ETLStateMachine`): `{app}-etl-pipeline-{env}` — orquesta el pipeline completo:
  1. `IngestEarthquakeData` (LambdaInvoke): invoca la Lambda de ingesta. Retry 3 veces con backoff x2 en errores de Lambda.
  2. `CheckRecordsCount` (Choice): si `recordsCount == 0` → salta a `NoRecordsSkipped` y termina. Si `recordsCount > 0` → continúa con el procesamiento Glue.
  3. `ProcessBronzeToSilver` (GlueStartJobRun): ejecuta el job de Glue con espera (`RUN_JOB` — sincrónico). Retry 3 veces con backoff x2 + jitter completo.
  4. `ProcessSilverToGold` (GlueStartJobRun): ejecuta el segundo job de Glue. Misma config de retry.
  - Timeout total: 4 horas. Trazabilidad X-Ray activada.
- **EventBridge Rule** (`ETLPipelineSchedule`): `{app}-etl-pipeline-schedule-{env}` — dispara la Step Functions **cada 6 horas** (misma frequencia que la ingesta).

**Conexiones**: Recibe `ingestion_lambda` (de IngestionStack), `bronze_to_silver_job` y `silver_to_gold_job` (de GlueStack), y `data_bucket`. Expone `state_machine` para MonitoringStack.

---

### 5. MonitoringStack

**Archivo**: `etl_cdk/stacks/monitoring_stack.py`
**ID**: `earthquake-etl-monitoring-{env}`
**Dependencias**: No tiene `add_dependency` explícita, pero recibe referencias de DataLake, Glue y Orchestration.

**Qué crea**:
- **SNS Topic** (`ETLAlerts`): `{app}-etl-alerts-{env}` — tópico para notificaciones de alertas.
- **CfnParameter** (`AlertEmail`): parámetro para configurar el email de alertas (default: `admin@example.com`). Se suscribe al SNS Topic con `EmailSubscription`.
- **Alarmas CloudWatch**:
  - `{app}-bronze-to-silver-failure-{env}`: se dispara si el job Bronze→Silver falla (métrica `glue.driver.aggregate.numFailedJobs` ≥ 1).
  - `{app}-silver-to-gold-failure-{env}`: igual para el job Silver→Gold.
  - `{app}-state-machine-failure-{env}`: se dispara si la Step Functions falla (métrica `ExecutionsFailed` ≥ 1).
  - `{app}-state-machine-duration-{env}`: se dispara si la ejecución supera las 3 horas (10800 segundos).
  - Todas las alarmas notifican al SNS Topic.
- **CloudWatch Dashboard** (`ETLDashboard`): `{app}-etl-dashboard-{env}` con 5 widgets:
  - Gráfica de ejecuciones Glue (Bronze→Silver): completados vs fallos
  - Gráfica de ejecuciones Glue (Silver→Gold): completados vs fallos
  - Gráfica de Step Functions: started vs succeeded vs failed
  - Duración del pipeline ETL (promedio)
  - Volumen de datos S3 (cantidad de objetos)

**Conexiones**: Recibe `data_bucket` (de DataLake), `bronze_to_silver_job` y `silver_to_gold_job` (de Glue), `state_machine` (de Orchestration).

---

### Diagrama de Flujo de Datos

```
USGS API
   │
   ▼
┌─────────────────────────────────────────────────────────────┐
│ IngestionStack (Lambda)         cada 6h via EventBridge     │
│ api_to_bronze.lambda_handler                                │
│   → escribe JSON crudo en s3://.../bronze/{fecha}/          │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ OrchestrationStack (Step Functions)   cada 6h via EventBridge│
│                                                             │
│  1. Invoke Lambda (ingesta)                                 │
│  2. ¿recordsCount > 0? ──no──→ Fin                          │
│         │ sí                                                │
│         ▼                                                   │
│  3. GlueStartJobRun: BronzeToSilver (Spark + Delta)         │
│         ▼                                                   │
│  4. GlueStartJobRun: SilverToGold (Spark + Delta)           │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ GlueStack (AWS Glue)                                        │
│                                                             │
│  BronzeToSilver:  s3://.../bronze/ → s3://.../silver/      │
│                    (limpia, transforma, escribe Delta)      │
│  SilverToGold:    s3://.../silver/ → s3://.../gold/        │
│                    (modelo dimensional, tablas Glue)        │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ MonitoringStack (CloudWatch + SNS)                          │
│  - Dashboard con métricas de Glue, Step Functions, S3       │
│  - Alarmas: fallos en jobs Glue, fallos en Step Functions,  │
│    duración excesiva del pipeline                            │
│  - Notificaciones por email vía SNS                         │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ DataLakeStack (S3)                                          │
│  bronze/ (crudos, expira 1 año)                             │
│  silver/ (limpios, expira 2 años)                           │
│  gold/   (modelados, expira 5 años)                         │
│  Todos cifrados con KMS, versionados, acceso público bloqueado│
└─────────────────────────────────────────────────────────────┘
```

### Riesgos / Observaciones

1. **Typo en nombre de script**: `prosses_silver_gold.py` en vez de `process_silver_gold.py`. No es un bug funcional mientras el archivo real se llame igual, pero vale la pena corregirlo para mantener consistencia.

2. **MonitoringStack sin dependencia explícita**: Aunque recibe referencias de otros stacks, no tiene `add_dependency()`. Esto significa que CloudFormation podría intentar crear recursos de monitoreo antes de que existan los jobs de Glue o la Step Functions. Sin embargo, como solo usa referencias (nombres, no ARNs complejos) probablemente funciona, pero no es ideal.

3. **AlertEmail default `admin@example.com`**: El parámetro tiene un default que no es válido para producción. En CI/CD deberían pasar siempre este parámetro.

4. **Costos**: Glue con 5 workers G.2X tiene un costo no menor (~$0.77/hora aprox. cada worker). Con ejecuciones cada 6 horas, el costo mensual puede ser significativo si los jobs se ejecutan siempre. El `max_concurrent_runs=1` evita ejecuciones paralelas accidentales, bien ahí.

5. **Delta Lake sin bucket dedicado**: Los checkpoints de Delta Lake van al mismo bucket de datos. Podría generar ruido en el bucket, pero es manejable.

### Listo para Propuesta

No — esto es una exploración informativa. Si se quiere proponer un cambio específico (como corregir el typo, añadir dependencias faltantes, optimizar costos, etc.), se necesitaría abrir un SDD change con ese objetivo.

---

**Status**: success
**Summary**: Exploración completa de los 5 stacks CDK del proyecto etl-earthquake-aws. Se analizaron sus responsabilidades, recursos creados, conexiones entre stacks y el flujo de datos del pipeline ETL.
**Artifacts**: `.atl/sdd/stacks-exploration/explore.md`
**Next**: null (exploration only)
**Risks**: Typo en nombre de script Glue (`prosses` vs `process`), MonitoringStack sin dependencia explícita, AlertEmail con default placeholder
**Skill Resolution**: paths-injected
