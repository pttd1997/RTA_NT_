# Lab 1 — Kafka: Producers, Consumers, Real-Time ETL

## Files
- `producer.py` — generates simulated e-commerce transactions, 1/sec
- `consumer_filter.py` — stateless: alerts on transactions > 3000 PLN

## Homework
- `homework/consumer_velocity.py` — stateful: velocity anomaly detector
  Alerts if the same user_id makes more than 3 transactions within 60 seconds.
