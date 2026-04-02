# Data Platform - High Level Design (HLD)

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                       🔄 INGESTION LAYER                             │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────────┐  │
│  │  Batch       │      │  Streaming   │      │  Data Collector  │  │
│  │  Sources     │─────▶│  Sources     │─────▶│  (Connectors)    │  │
│  │  (Files, DB, │      │  (Kafka,     │      └──────────────────┘  │
│  │   DW)        │      │   Kinesis)   │                             │
│  └──────────────┘      └──────────────┘                             │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       📦 STAGING LAYER                               │
│  ┌──────────────────┐       ┌──────────────────┐                   │
│  │  Staging Zone    │──────▶│  Data Validation │                   │
│  │  (Raw Data Lake) │       │  & Parsing       │                   │
│  └──────────────────┘       └──────────────────┘                   │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       ⚙️ PROCESSING LAYER                            │
│  ┌──────────────┐  ┌─────────────┐  ┌──────────────────┐           │
│  │  Spark Jobs  │─▶│   Data QA   │─▶│   Aggregation &  │           │
│  │  (ETL/ELT)   │  │   Checks    │  │   Transform      │           │
│  └──────────────┘  └─────────────┘  └──────────────────┘           │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       💾 STORAGE LAYER                               │
│  ┌──────────────────────┐  ┌─────────────────────┐                 │
│  │   Delta Lake         │  │  Metadata Store     │                 │
│  │  (Bronze/Silver/Gold)│◀─│  (Hive Metastore)   │                 │
│  └──────────────────────┘  └─────────────────────┘                 │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       📊 SERVING LAYER                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐           │
│  │   Query      │  │   REST APIs  │  │   BI Tools       │           │
│  │   Engine     │──│   (Data API) │  │   (Power BI)     │           │
│  │  (SparkSQL)  │  └──────────────┘  └──────────────────┘           │
│  └──────────────┘                                                    │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                 📈 MONITORING & GOVERNANCE                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐           │
│  │ Observability│  │   Data       │  │  Security &      │           │
│  │ (Logs,       │  │   Governance │  │  Auth (Access    │           │
│  │  Metrics)    │  │  (Lineage)   │  │   Control)       │           │
│  └──────────────┘  └──────────────┘  └──────────────────┘           │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Components

### 1. Ingestion Layer
- **Batch Sources**: Files (CSV, Parquet), Relational databases, Data warehouses
- **Streaming Sources**: Kafka, AWS Kinesis, Event hubs
- **Data Collectors**: Using connectors (Fivetran, Talend, Custom)

### 2. Staging Layer
- **Raw Data Zone**: Minimal transformation, schema preservation
- **Validation & Parsing**: Data quality checks, format conversion

### 3. Processing Layer
- **Apache Spark**: Distributed processing engine for ETL/ELT
- **Data Quality Checks**: Validation, null checks, deduplication
- **Aggregation & Transformation**: Business logic, enrichment, calculations

### 4. Storage Layer
- **Delta Lake**: ACID transactions, time travel, unified data
  - Bronze: Raw ingested data
  - Silver: Cleaned, validated data
  - Gold: Business-ready analytics data
- **Metadata Store**: Hive Metastore for schema management

### 5. Serving Layer
- **Query Engine**: Spark SQL for interactive queries
- **REST APIs**: Programmatic access to data
- **BI Tools**: Power BI for visualization and dashboards

### 6. Cross-Cutting Concerns
- **Observability**: Logging, metrics, tracing
- **Data Governance**: Lineage, data catalog, compliance
- **Security**: Authentication, authorization, encryption
