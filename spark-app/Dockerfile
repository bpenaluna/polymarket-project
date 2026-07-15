FROM bitnamilegacy/spark:3.5.5

USER root

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY stream-processor.py /opt/bitnami/spark/
COPY view_data.py /opt/bitnami/spark/

RUN mkdir -p /opt/bitnami/spark/data /opt/bitnami/spark/checkpoints

WORKDIR /opt/bitnami/spark/