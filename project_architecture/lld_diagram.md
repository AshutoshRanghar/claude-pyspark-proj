# Data Platform - Low Level Design (LLD)

## Detailed Technical Architecture

### INGESTION LAYER (DETAILED)

**BATCH INGESTION SOURCES:**
- SQL Server, PostgreSQL, MySQL, MongoDB, Oracle Database
- AWS S3, Azure Data Lake Storage (ADLS), Google Cloud Storage
- CSV, Parquet, JSON, ORC, Delta format files
- Data warehouses (Snowflake, Redshift, BigQuery)

**CONNECTION METHODS:**
- JDBC/ODBC drivers for relational databases
- Cloud SDK APIs (AWS S3 SDK, Azure SDK)
- Custom connectors (Fivetran, Talend, Stitch)
- REST APIs with pagination support

**STREAMING INGESTION SOURCES:**
- Apache Kafka (Topics, partitions with consumer groups)
- AWS Kinesis (Streams with shards)
- Azure Event Hubs (Partitions with consumer groups)
- RabbitMQ, AWS SQS/SNS
- HTTP webhooks for event-driven data

**INGESTION ENGINE:**
- Apache NiFi for complex routing
- Kafka Connect for scalable ingestion
- Apache Spark Structured Streaming for unified processing
- Custom Python/Scala connectors

---

### STAGING LAYER (DETAILED)

**LANDING ZONE STRUCTURE:**
```
ADLS / S3 Path Structure:
/raw/{source}/{domain}/{load_date}/
├─ 2024-04-01/
│  ├─ part-00000.parquet
│  ├─ part-00001.parquet
│  └─ _SUCCESS
├─ _delta_log/
│  ├─ 00000000000000000000.json (Delta transaction log)
│  └─ 00000000000000000001.json
└─ metadata.json
```

**VALIDATION CHECKS:**
- Schema validation against expected structure
- Data type verification (string ≠ numeric)
- Null/empty field detection
- Duplicate record identification by primary key
- File integrity checks (row count, file size)

**STANDARDIZATION PROCESS:**
- CSV → Parquet conversion
- JSON flattening (nested → flat schema)
- Datetime format parsing (ISO 8601)
- Character encoding normalization (UTF-8)
- Column name standardization (snake_case)

**DATA QUALITY METRICS CAPTURED:**
- Record count ingested
- Schema mismatches detected
- Duplicate rows found
- Null percentage per column

---

### PROCESSING LAYER (DETAILED)

**SPARK CLUSTER CONFIGURATION:**
- Compute: Databricks, AWS EMR, Azure HDInsight, or on-prem
- Driver: 16 GB RAM, 4 cores
- Executors: 8 GB RAM, 2 cores (auto-scaling 2-50)
- Storage: High-bandwidth SSD
- Network: 10+ Gbps connectivity

**BATCH ETL JOBS (PySpark / Spark SQL):**

1. **Data Cleansing Job:**
   - Input: /raw/ (Bronze)
   - Transformations:
     - Remove duplicate rows (by business key)
     - Handle missing values (nulls/blanks)
     - Standardize data types
     - Detect and flag outliers
     - Remove invalid records
   - Output: /silver/ (Silver)

2. **Business Logic & Aggregation Job:**
   - Input: /silver/ (cleaned dimensions + facts)
   - Transformations:
     - Star schema joins (Dimension + Fact)
     - Aggregations (SUM, COUNT, AVG, PERCENTILE)
     - Window functions (ROW_NUMBER, LAG, LEAD)
     - Feature engineering (ratios, rankings)
     - Slowly Changing Dimensions (SCD Type 1/2)
   - Output: /gold/ (Analytics-ready)

3. **Incremental Load Strategy:**
   - Change Data Capture (CDC) from sources
   - Merge operations (INSERT/UPDATE/DELETE)
   - Delta Lake MERGE INTO statements
   - Upsert logic by primary key

**STREAMING JOBS (Spark Structured Streaming):**
- Real-time Kafka/Kinesis consumption
- Windowed aggregations:
  - Tumbling windows (5min, 1hr)
  - Sliding windows (overlapping intervals)
  - Session windows (event-driven groups)
- Stateful operations (COUNT, MAX, MIN)
- Output modes: Append, Update, Complete
- Write to Delta tables with streaming transactions

**DATA QUALITY FRAMEWORK:**
- Tool: Great Expectations / dbt tests
- Completeness: % of null values per column
- Accuracy: Value ranges, regex patterns
- Consistency: Cross-table referential integrity
- Freshness: Maximum data age threshold
- Uniqueness: Primary key constraints
- Row count validation

**ORCHESTRATION & SCHEDULING:**
- Apache Airflow DAGs
- Job dependencies (serial/parallel execution)
- Retry logic (exponential backoff)
- Error notifications (Slack, PagerDuty)
- SLA monitoring (max execution time)
- Backfill support for historical data

---

### STORAGE LAYER (DETAILED)

**DELTA LAKE MEDALLION ARCHITECTURE:**

**BRONZE LAYER (Raw Data):**
- **Path:** `/delta/bronze/{domain}/{table}/`
- **Format:** Parquet with Delta transaction log
- **Schema:** Exact source schema (minimal changes)
- **Partitioning:** By `load_date` (YYYY-MM-DD)
- **Secondary Partitions:** By `source_system`
- **Retention:** 90 days (audit trail)
- **Example:** `/delta/bronze/sales/orders/2024-04-01/`
- **Schema Evolution:** Additive changes supported
- **Compaction:** Z-order by `order_id` for joins

**SILVER LAYER (Cleaned & Deduplicated):**
- **Path:** `/delta/silver/{domain}/{table}/`
- **Format:** Parquet with Delta protocol
- **Schema:** Standardized, type-safe schema
- **Transformations Applied:**
  - Type casting: `string → timestamp`, `int → decimal`
  - Deduplication by business key
  - Null handling: defaults or imputation
  - Column renaming: snake_case convention
  - Reference data joins (e.g., dim_country)
- **Partitioning:** By `partition_date` + `domain`
- **Retention:** 1+ year (operational data)
- **Z-Order Columns:** Frequently filtered columns
- **Data Quality:** 99%+ quality score

**GOLD LAYER (Analytics-Ready):**
- **Path:** `/delta/gold/{domain}/{mart_name}/`
- **Schema:** Business-aligned, denormalized
- **Fact Tables:**
  - `fact_sales`: Transactional, daily grain
  - `fact_revenue`: Pre-aggregated monthly/quarterly
- **Dimension Tables (SCD Type 2):**
  - `dim_customer`: Slowly changing (address, segment)
  - `dim_product`: SCD Type 1 (overwrite attributes)
  - `dim_date`: Surrogate keys, fiscal periods
- **Partitioning:** By year/month for performance
- **Pre-aggregations:** Summary tables for dashboards
- **Indexing:** Z-order by primary filtering columns

**HIVE METASTORE INTEGRATION:**
- **External Tables:** Point to Delta tables
- **Partition Management:** Auto-sync with Delta
- **Column Lineage:** Tracked via Spark catalyst
- **Table Statistics:** Cardinality, row count
- **Catalog Tools:** Apache Atlas, Collibra, OpenMetadata

**DELTA LAKE FEATURES UTILIZED:**
- **ACID Transactions:** Multi-version concurrency control
- **Time Travel:** Query historical data versions
- **Schema Enforcement:** Prevent schema mismatches
- **Data Versioning:** Complete audit trail
- **Unified Batch/Streaming:** Same storage format

---

### SERVING LAYER (DETAILED)

**QUERY ENGINES:**
1. **Spark SQL (Batch)**
   - Distributed query execution
   - Catalyst optimizer
   - Support for complex joins/aggregations
   - Latency: <5s for aggregated queries

2. **Presto / Trino (MPP)**
   - Fast ad-hoc query execution
   - Multiple data source connectors
   - Federated queries across systems
   - Latency: <2s for indexed queries

3. **Serverless (Athena, Synapse)**
   - Pay-per-query model
   - Auto-scaling
   - Partition pruning for cost optimization

**DATA API LAYER:**
- **REST API Endpoints:**
  - `GET /api/v1/tables` - List available tables
  - `GET /api/v1/data/{table}` - Fetch data with filters
  - `POST /api/v1/query` - Execute SQL queries
  - `GET /api/v1/lineage/{table}` - Data lineage

- **Response Format:** JSON, Parquet, CSV
- **Authentication:** OAuth2, JWT tokens
- **Authorization:** Row-level security (RLS)
- **Rate Limiting:** 1000 req/min per API key
- **Caching:** Redis (5min TTL for public queries)
- **Pagination:** Cursor-based, 10K rows/page

**BI TOOLS INTEGRATION:**
- **Power BI:**
  - Direct Query mode (live connections)
  - Import mode (cached datasets)
  - Paginated reports for distribution
  - Row-level security (RLS)
  
- **Tableau:**
  - Live connections via ODBC/JDBC
  - Embedded analytics
  - Automated refreshes (hourly)

- **Looker/Tableau:**
  - LookML / Tableau Prep for data transformation
  - Alerts & thresholds
  - Drill-through capabilities

**CACHING STRATEGY:**
- **Delta Cache (Spark Native):** Accelerates repeated queries
- **Redis Cache:** Query result caching (5min)
- **Bitmap Indexes:** Fast filtering on large columns
- **Bloom Filters:** Probabilistic filtering

---

### OBSERVABILITY & GOVERNANCE (DETAILED)

**LOGGING & MONITORING:**
- **Centralized Logging:** ELK Stack (Elasticsearch, Logstash, Kibana)
  - Spark driver/executor logs
  - Job status transitions
  - Error stack traces
  - Query execution plans

- **Metrics Collection:** Prometheus + Grafana
  - CPU/Memory/Disk utilization
  - Network I/O throughput
  - Query latency percentiles (p50, p95, p99)
  - Data volume metrics (rows/sec)
  - Custom KPIs (freshness, quality %)

- **Alerting:**
  - PagerDuty: Critical alerts (SLA breach)
  - Slack: Info/warning notifications
  - Thresholds: Job failure, data quality <95%

**DATA GOVERNANCE:**
- **Data Catalog:** Apache Atlas / Collibra
  - Table & column metadata
  - Data ownership assignment
  - Sensitivity tags (PII, Confidential)
  - Business glossary mapping

- **Data Lineage Tracking:**
  - Upstream sources (input tables)
  - Transformation DAGs (Spark job steps)
  - Downstream consumers (reports, APIs)
  - Column-level lineage (impact analysis)

- **Data Quality SLOs:**
  - Completeness: >99% non-null required fields
  - Accuracy: >99.5% quality score
  - Timeliness: Daily data within 6 hours
  - Freshness: <24 hour data age

**SECURITY & COMPLIANCE:**
- **Encryption:**
  - At-rest: AES-256 (ADLS, S3, Delta tables)
  - In-transit: TLS 1.2+
  - Key management: Azure Key Vault / AWS KMS

- **Access Control:**
  - RBAC (Role-Based Access Control)
  - Column-level security
  - Row-level security (RLS)
  - Service principals for automation

- **Audit Logging:**
  - Query audit trail (who, what, when)
  - Data access logs
  - Schema change history
  - Compliance reports (GDPR, HIPAA)

- **Data Masking:**
  - PII redaction (email, SSN)
  - Tokenization for sensitive data
  - Dynamic masking based on roles

---

## Performance & SLA Targets

**Data Freshness:**
- Batch loads: Daily, within 6 hours of day close
- Streaming: Real-time, <5 minute latency
- User-facing dashboards: <15 minutes

**Query Performance:**
- Aggregated (Gold) queries: <2 seconds (p99)
- Ad-hoc (Silver) queries: <5 seconds (p99)
- Complex joins (Bronze): <10 seconds (p99)

**Availability:**
- API uptime: 99.95% (quarterly)
- Data availability: 99.9% (4 9s)
- Job success rate: >95% without intervention

**Data Quality:**
- Quality score: >99% (Completeness + Accuracy)
- Deduplication rate: <0.1% duplicates
- Schema validation: 100% pass rate

**Infrastructure:**
- Spark cluster uptime: 99.5%
- Storage redundancy: 3x replication (ADLS/S3)
- Disaster recovery: RTO 4 hours, RPO 1 hour

---

## Technology Stack Summary

| Layer | Component | Technology |
|-------|-----------|-----------|
| **Ingestion** | Batch Connector | Fivetran, Talend, Custom |
| **Ingestion** | Streaming Broker | Apache Kafka, AWS Kinesis |
| **Staging** | Storage | Azure ADLS, AWS S3 |
| **Processing** | Compute | Apache Spark 3.x |
| **Processing** | Orchestration | Apache Airflow |
| **Processing** | Language | PySpark, Scala, SQL |
| **Storage** | Format | Delta Lake (Parquet) |
| **Storage** | Metadata | Hive Metastore, Atlas |
| **Serving** | Query Engine | Spark SQL, Presto |
| **Serving** | API | FastAPI, Flask |
| **Serving** | BI Tools | Power BI, Tableau |
| **Observability** | Logging | ELK Stack |
| **Observability** | Metrics | Prometheus, Grafana |
| **Governance** | Catalog | Apache Atlas |
| **Governance** | Quality | Great Expectations |
| **Security** | Auth | OAuth2, JWT |
