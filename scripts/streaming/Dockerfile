FROM bitnami/spark:3.5.0

USER root

WORKDIR /app
COPY streaming_requirements.txt .
RUN pip install --no-cache-dir -r streaming_requirements.txt

COPY streaming_processor.py .

user spark