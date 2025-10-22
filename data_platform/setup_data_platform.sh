#!/bin/sh
echo "Starting data platform setup..."  && \
python data_platform/scripts/run_ingestion.py  && \
python data_platform/scripts/run_etl.py  && \
echo "Data platform setup completed."