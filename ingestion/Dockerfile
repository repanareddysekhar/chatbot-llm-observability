FROM python:3.12-slim

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install psycopg2 for Celery sync tasks
RUN pip install psycopg2-binary

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy SDK so Celery tasks can import llm_obs.pii
COPY ../sdk /app/sdk
RUN pip install --no-cache-dir /app/sdk

COPY . .

# Entrypoint: run migrations then start server (or worker, overridden via compose command)
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 4000
ENTRYPOINT ["/entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "4000"]
