# Multi-stage Dockerfile for ETL Pipeline & Analytics Web Server
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Initialize data and run pipeline to seed warehouse
RUN python run_pipeline.py

# Expose web dashboard port
EXPOSE 8080

# Environment variables
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# Start the web dashboard and analytics server
CMD ["python", "app.py"]
