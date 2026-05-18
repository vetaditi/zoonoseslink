FROM python:3.12-slim

WORKDIR /app

# System deps for psycopg2 and shapely
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev gdal-bin libgdal-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000
