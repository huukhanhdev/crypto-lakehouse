FROM python:3.12-slim

WORKDIR /app

# psycopg2-binary ships wheels; no build deps needed
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest.py .

# unbuffered so logs stream to `docker compose logs`
ENV PYTHONUNBUFFERED=1

CMD ["python", "ingest.py"]
