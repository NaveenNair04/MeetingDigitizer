FROM bitnami/spark:latest
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY spark-app.py .

