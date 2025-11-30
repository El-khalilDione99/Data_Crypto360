FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl_hdfs_to_postgres.py .

CMD ["python", "etl_hdfs_to_postgres.py"]
