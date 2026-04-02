# Data Platform Architecture - Quick Reference Guide

## System Overview

```
SOURCES → INGESTION → STAGING → PROCESSING → STORAGE → SERVING → USERS
(Batch)    (Connectors)  (Raw)   (Spark ETL)  (Delta)   (APIs/BI)
(Stream)   (Kafka/Kinesis) (Validation) (Quality) (Bronze/Silver/Gold)
```

---

## Layer-by-Layer Quick Reference

### 1️⃣ INGESTION LAYER
**What:** Get data from sources  
**Tools:** Fivetran, Kafka, Spark Connectors  
**Delivery:** Batch (daily) + Streaming (real-time)  
**Frequency:** Daily at 2 AM + continuous streams  
**SLA:** <6 hours from source change

### 2️⃣ STAGING LAYER
**What:** Store raw data, validate schema  
**Location:** `/raw/{source}/{domain}/{date}/` in ADLS/S3  
**Format:** Parquet (partitioned by date)  
**Retention:** 90 days  
**Quality Checks:** Schema, nulls, duplicates

### 3️⃣ PROCESSING LAYER (Apache Spark)
**What:** Clean, transform, aggregate data  
**Jobs:** Cleansing → Business Logic → Aggregation  
**Language:** PySpark, Scala, SQL  
**Orchestration:** Apache Airflow DAGs  
**Execution:** Spark 3.5+ (16GB driver, 8GB executors)  
**Quality Framework:** Great Expectations, dbt tests

### 4️⃣ STORAGE LAYER (Delta Lake)
**What:** ACID-compliant unified storage  
**Format:** Parquet + Delta protocol  
**Layers:**
- **Bronze:** Raw data, 90-day retention
- **Silver:** Cleaned, deduplicated, 1+ year
- **Gold:** Analytics-ready, fully denormalized

**Paths:**
```
/delta/bronze/{domain}/{table}/2024-04-01/part-*.parquet
/delta/silver/{domain}/{table}/2024-04-01/part-*.parquet
/delta/gold/{domain}/{mart}/fact_*.parquet
```

**Key Features:** ACID, time-travel, schema evolution, unified API

### 5️⃣ SERVING LAYER
**Query Engines:** Spark SQL, Presto, Athena  
**APIs:** REST (JSON), GraphQL  
**BI Tools:** Power BI, Tableau, Looker  
**Caching:** Redis (5-min TTL)  
**Rate Limit:** 1000 req/min per API key

### 6️⃣ OBSERVABILITY & GOVERNANCE
**Logging:** ELK Stack (Elasticsearch, Logstash, Kibana)  
**Metrics:** Prometheus + Grafana  
**Lineage:** Apache Atlas / OpenLineage  
**Quality:** Data quality dashboards  
**Security:** RBAC, RLS, encryption (AES-256)

---

## Data Model Reference

### Fact Tables (Gold Layer)
```sql
-- Transactional grain (one row = one transaction)
fact_sales
├─ order_id (PK)
├─ customer_id (FK → dim_customer)
├─ product_id (FK → dim_product)
├─ order_date (FK → dim_date)
├─ amount
├─ quantity
└─ load_date

-- Aggregated grain (pre-computed summaries)
fact_revenue_monthly
├─ month
├─ customer_segment (FK → dim_customer_segment)
├─ total_revenue (SUM)
├─ order_count (COUNT)
└─ avg_order_value (AVG)
```

### Dimension Tables (Gold Layer)
```sql
-- Slowly Changing Dimension Type 2 (tracks history)
dim_customer
├─ customer_id (PK, Surrogate)
├─ customer_src_id (Natural key)
├─ customer_name
├─ segment
├─ country
├─ valid_from (When this record became effective)
├─ valid_to (When this record expired)
└─ is_current (Boolean: true if active)

-- Slowly Changing Dimension Type 1 (overwrites)
dim_product
├─ product_id (PK)
├─ product_name
├─ category
├─ price (Latest price, overwrites)
└─ last_updated
```

---

## File Structure Reference

```
project_architecture/
├── DATA_PLATFORM_ARCHITECTURE.md
│   ├─ Executive Summary
│   ├─ 1. Architecture Overview (HLD)
│   ├─ 2-7. Layer Details (HLD + LLD)
│   ├─ 8. Component Specifications (LLD)
│   ├─ 9. Technology Stack
│   ├─ 10. Deployment Architecture
│   ├─ 11-16. Operations & Procedures
│   └─ Use: Comprehensive reference (50+ pages)
│
├── hld_diagram.md
│   ├─ High-level boxes & flows
│   ├─ Component descriptions
│   └─ Use: Stakeholder presentations
│
├── lld_diagram.md
│   ├─ Detailed technical specifications
│   ├─ Data flow details
│   ├─ Technology choices
│   ├─ Performance targets
│   └─ Use: Development team reference
│
└── ARCHITECTURE_QUICK_REFERENCE.md (this file)
    ├─ One-page summaries per layer
    ├─ Data model reference
    ├─ Quick lookup tables
    └─ Use: Daily developer reference
```

---

## Common Tasks & Quick Answers

### "How do I ingest new data from System X?"
1. **Batch source:** Use Fivetran connector or custom Spark job
2. **Streaming source:** Set up Kafka topic + Kafka Connect sink
3. **Path:** Land in `/raw/{system_name}/{table_name}/YYYY-MM-DD/`
4. **Scheduling:** Add to Airflow DAG with dependencies

### "Where is my data stored after ingestion?"
- **Immediately:** `/raw/{source}/{domain}/{date}/` (Bronze)
- **After cleaning:** `/silver/{domain}/{table}/` (Silver)
- **After aggregation:** `/gold/{domain}/{mart}/` (Gold)

### "How do I query the data?"
- **SQL:** `SELECT * FROM gold.fact_sales WHERE order_date = '2024-04-01'`
- **API:** `GET /api/v1/data/fact_sales?filter=order_date:2024-04-01`
- **BI:** Connect Power BI to Delta Lake via Spark SQL endpoint

### "Is my data fresh?"
Check Grafana dashboard: `grafana.internal/d/freshness`
- ✅ Green: Data loaded within SLA (6 hours)
- 🟡 Yellow: Data loaded but outside SLA
- 🔴 Red: Data NOT loaded today

### "What's the quality of my dataset?"
Check data catalog: `atlas.internal/dataset/{name}`
- Quality score: % rows passing all validation rules
- Target: >99%
- Issues: Quarantined bad rows with error codes

### "Who's using my dataset?"
Check lineage in Atlas:
- **Downstream:** Reports, dashboards, APIs
- **Upstream:** Source systems, ETL jobs
- **Impact:** Who's affected if data changes

### "How do I debug a failed job?"
1. **Airflow:** Check DAG run in UI
2. **Spark:** View executor logs in ELK dashboard
3. **Data:** Query `/raw/` or `/silver/` directly
4. **Quality:** Check Great Expectations report

---

## Performance Checklists

### Slow Query Diagnosis
- [ ] Check partition pruning (filter by partition column)
- [ ] Review query plan (EXPLAIN output)
- [ ] Add Z-ordering if filtering high-cardinality column
- [ ] Verify table statistics are up-to-date
- [ ] Consider materialized views for repeated aggregations

### Slow Ingest Diagnosis
- [ ] Check source system (is it slow?)
- [ ] Review network connectivity
- [ ] Increase parallelism (more partitions)
- [ ] Check Spark executor memory/CPU
- [ ] Review transformation complexity

### High Storage Costs
- [ ] Enable partitioning (query fewer files)
- [ ] Use compression (Parquet snappy)
- [ ] Reduce retention (drop old data)
- [ ] Move cold data to Glacier
- [ ] Deduplicate Bronze layer

---

## Security Checklists

### Data Access
- [ ] User has service principal in Azure/AWS
- [ ] Service principal has read permission on Delta table
- [ ] Row-level security (RLS) applied if sensitive
- [ ] Column masking enabled for PII
- [ ] Audit logging enabled

### Data Movement
- [ ] Data exported over HTTPS/TLS only
- [ ] No PII exported to unsecured locations
- [ ] Encryption at rest (AES-256) for cloud storage
- [ ] Network policies restrict egress
- [ ] Data classification tags applied

### Compliance
- [ ] GDPR: Right to deletion implemented
- [ ] HIPAA: Encryption + audit trails
- [ ] SOX: Change management procedures
- [ ] Data residency: Data in correct region

---

## SLA & Monitoring Reference

### Service Level Objectives (SLOs)

| Metric | Target | Measurement | Dashboard |
|--------|--------|-------------|-----------|
| **Freshness** | Daily within 6h | Latest load timestamp | `grafana:/freshness` |
| **Availability** | 99.9% (3 9s) | Uptime % | `grafana:/availability` |
| **Query Latency** | Gold <2s, Silver <5s | p99 latency | `grafana:/latency` |
| **Quality** | >99% | % rows passing validation | `atlas:/quality` |
| **Cost** | $X/TB/month | Total monthly cost | `chargeback:/report` |

### Critical Alerts (Page Oncall)
- 🚨 Data not loaded within SLA (>6 hours late)
- 🚨 Quality score drops below 95%
- 🚨 API availability < 99% (3 9s)
- 🚨 Critical job failure (retry exhausted)

### Warning Alerts (Post in Slack)
- ⚠️ Job took 2x expected duration
- ⚠️ Quality degradation trend
- ⚠️ Storage growth >10% month-over-month
- ⚠️ Query latency degradation

---

## Contact & Escalation

**Data Platform Team:** data-platform@company.com  
**On-Call Rotation:** pagerduty.com/oncall  
**Incident Channel:** #data-incidents (Slack)  
**Documentation:** this-repo/docs  
**Data Catalog:** atlas.internal  
**Monitoring:** grafana.internal  
**API Docs:** api-docs.internal/swagger

---

## Additional Resources

📖 **Full Documentation:** `DATA_PLATFORM_ARCHITECTURE.md`  
📊 **HLD Diagrams:** `hld_diagram.md`  
⚙️ **LLD Details:** `lld_diagram.md`  
🚀 **Implementation Guide:** Implementation runbook (separate doc)  
🔍 **Troubleshooting Guide:** Troubleshooting manual (separate doc)  

---

**Last Updated:** April 2024  
**Architecture Version:** 1.0  
**Maintained By:** Data Engineering Team
