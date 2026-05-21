FROM python:3.12-slim

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install psycopg2 for Celery sync tasks
RUN pip install psycopg2-binary

# Copy SDK first (from project root)
COPY sdk /app/sdk
RUN pip install --no-cache-dir /app/sdk

# Copy ingestion service
COPY ingestion/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ingestion/app /app/app
COPY ingestion/alembic /app/alembic
COPY ingestion/alembic.ini /app/alembic.ini
COPY ingestion/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 4000
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "4000"]
