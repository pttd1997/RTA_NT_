# DAX reference — Realtime_Fraud_Monitoring.pbix

All measures live in the `public processed_transactions` table.

## KPIs

```dax
Total Transactions =
COUNTROWS('public processed_transactions')

High Risk Alerts =
CALCULATE(
    COUNTROWS('public processed_transactions'),
    'public processed_transactions'[risk_level] = "HIGH"
)

Medium Risk Transactions =
CALCULATE(
    COUNTROWS('public processed_transactions'),
    'public processed_transactions'[risk_level] = "MEDIUM"
)

Average Fraud Score =
AVERAGE('public processed_transactions'[fraud_score])

Financial Volume Blocked (PLN) =
CALCULATE(
    SUM('public processed_transactions'[amount]),
    'public processed_transactions'[risk_level] = "HIGH"
)

Fraud_Rate =
DIVIDE(
    COUNTROWS('public fraud_alerts'),
    COUNTROWS('public processed_transactions'),
    0
)
```

## Streaming status

```dax
Latest Transaction Time =
MAX('public processed_transactions'[transaction_timestamp])

Current Time =
UTCNOW()

Time_Gap_Fixed =
VAR Latest_Transaction = MAX('public processed_transactions'[transaction_timestamp])
-- Use UTCNOW() instead of NOW() so the time zone matches Spark/PostgreSQL (UTC)
VAR Current_Time = UTCNOW()
VAR Gap_In_Seconds = DATEDIFF(Latest_Transaction, Current_Time, SECOND)
RETURN
SWITCH(
    TRUE(),
    ISBLANK(Latest_Transaction), "Waiting for Data...",
    Gap_In_Seconds < 0, "Just now",
    Gap_In_Seconds < 60, Gap_In_Seconds & "s ago",
    Gap_In_Seconds < 3600, INT(Gap_In_Seconds / 60) & "m ago",
    INT(Gap_In_Seconds / 3600) & "h ago"
)
```

## Calculated columns

```dax
Hour       = HOUR([transaction_timestamp])
Minute     = MINUTE('public processed_transactions'[transaction_timestamp])
Time Label = FORMAT([transaction_timestamp], "HH:mm")
```

## Calculated table

```dax
FraudReasonsExpanded =
SELECTCOLUMNS(
    ADDCOLUMNS(
        GENERATE(
            'public processed_transactions',
            SELECTCOLUMNS(GENERATESERIES(1, 10, 1), "idx", [Value])
        ),
        "reason", TRIM(
            PATHITEM(SUBSTITUTE([fraud_reasons], ", ", "|"), [idx], TEXT)
        )
    ),
    "reason", [reason]
)
```
