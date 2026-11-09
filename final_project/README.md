# Final Project — Real-Time E-Commerce Fraud Detection & Monitoring

Group project for the Real-Time Analytics course at SGH Warsaw School of Economics.

Synthetic payment transactions are streamed through **Kafka**, scored against fraud rules in **Spark Structured Streaming**, stored in **PostgreSQL**, and monitored in a **Power BI** dashboard connected to the database.

## Team & my role

Team project. I built the Power BI dashboard: connected Power BI to the PostgreSQL database and developed the data model, DAX measures and report.

## Architecture

```
Producer (Python) ──► Kafka topic: transactions ──► Spark fraud engine ──┬──► PostgreSQL: processed_transactions ──┐
                                                                         │                                        ├──► Power BI
                                                                         └──► Kafka topic: fraud_alerts           │
                                                                              └─► PostgreSQL: fraud_alerts ───────┘
```

![Architecture: Producer → Kafka → Spark → PostgreSQL → Power BI](final_project/notebooks/architecture.png)]

Everything runs locally in Docker: Kafka broker, PostgreSQL 16, Redis, and a Jupyter/PySpark container.

## Repository contents

| Path | Purpose |
|---|---|
| `compose.yaml`, `Dockerfile` | Docker environment (Kafka, PostgreSQL, Redis, Jupyter + PySpark) |
| `notebooks/producer.py` | Generates synthetic PLN transactions (5,000 users, 8 merchant categories, 8 countries) and sends them to Kafka |
| `notebooks/fraudengine.py` | Spark Structured Streaming job: parses JSON, scores each transaction, writes results to PostgreSQL and high-risk alerts to Kafka |
| `notebooks/database_objects.ipynb` | Creates the PostgreSQL tables `processed_transactions` and `fraud_alerts` |
| `dashboard/Realtime_Fraud_Monitoring.pbix` | Power BI monitoring dashboard |
| `dashboard/dax_measures.md` | All DAX measures, calculated columns and the calculated table |

## Fraud scoring rules

| Rule | Points |
|---|---|
| Amount > 3,000 PLN | 30 |
| New device | 20 |
| Transaction country ≠ user country | 20 |
| Night transaction (00:00–05:59) | 15 |
| Risky merchant category (electronics, gaming, travel) | 15 |

| Total score | Risk level |
|---|---|
| 0–29 | LOW |
| 30–49 | MEDIUM |
| 50–74 | HIGH |
| 75+ | CRITICAL |

HIGH and CRITICAL transactions are published to the `fraud_alerts` Kafka topic and stored in the `fraud_alerts` table.

## How to run

```bash
docker compose up -d                       # start broker, postgres, redis, jupyter
# open Jupyter at http://localhost:8999 (token: root)
# run notebooks/database_objects.ipynb once to create the tables
python producer.py                         # in a Jupyter terminal, from the notebooks folder
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2,org.postgresql:postgresql:42.7.3 fraudengine.py
```

Check the data:

```sql
-- psql -h localhost -U frauduser -d frauddb
SELECT COUNT(*) FROM processed_transactions;
SELECT COUNT(*) FROM fraud_alerts;
```

The database credentials in `compose.yaml` are local development defaults for the Docker container only.

## Power BI dashboard

The dashboard connects to PostgreSQL (`localhost:5432`, database `frauddb`) and loads `processed_transactions` and `fraud_alerts`. The saved `.pbix` holds a snapshot of **151,071 processed transactions** and **9,812 fraud alerts**, so it can be explored without running the pipeline. Refreshing needs the Docker stack running.

### Data model

| Table | Source | Description |
|---|---|---|
| `processed_transactions` | PostgreSQL | Every scored transaction: amount, merchant category, payment method, device, countries, `fraud_score`, `risk_level`, `fraud_reasons` |
| `fraud_alerts` | PostgreSQL | HIGH and CRITICAL transactions |
| `FraudReasonsExpanded` | DAX calculated table | One row per individual fraud reason, split from the comma-separated `fraud_reasons` field |

Relationship: `fraud_alerts[transaction_id]` → `processed_transactions[transaction_id]` (1:1).

### Report contents

- **KPI cards:** Total Transactions, High Risk Alerts, Medium Risk Transactions, Average Fraud Score, Financial Volume Blocked (PLN)
- **Streaming status:** latest transaction time and a "time since last transaction" freshness label
- **Fraud Trend Over Time (Last 60 Minutes):** transaction volume with fraud rate
- **Risk Level Distribution** and **Average Fraud Score** by country and risk level
- **Fraud Alerts by Transaction Country** (map) and **by Merchant Category**
- **Fraud reasons breakdown**
- **Latest High-Risk Transactions** table for investigation
- Slicers for Risk Level and Merchant Category, plus a bookmark reset button

![REAL-TIME ECOMMERCE FRAUD DETECTION DASHBOARD](dashboard/Dashboard_RTA_%20Final.png)

### DAX highlights

Full list in [`dashboard/dax_measures.md`](dashboard/dax_measures.md).

- **Fraud_Rate:** `DIVIDE` of alerts over all transactions, safe against division by zero
- **Financial Volume Blocked (PLN):** `CALCULATE` with a risk-level filter
- **Time_Gap_Fixed:** `VAR` + `SWITCH` freshness label ("12s ago", "3m ago"), using `UTCNOW()` because Spark writes UTC timestamps
- **FraudReasonsExpanded:** calculated table using `GENERATE`, `GENERATESERIES` and `PATHITEM` to split a multi-value text field into rows
