# Minimal dbt-trino runner. Pinned so `dbt build` is reproducible offline.
FROM python:3.11-slim

RUN pip install --no-cache-dir "dbt-trino==1.8.4"

WORKDIR /dbt
ENV DBT_PROFILES_DIR=/dbt
ENTRYPOINT ["dbt"]
CMD ["build"]
