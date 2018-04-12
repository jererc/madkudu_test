#!/usr/bin/env bash
$SPARK_HOME/bin/spark-submit \
    --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.1 \
    aggregate_pages.py
