# Use a trusted base image that has Java and Spark pre-installed
FROM bitnamilegacy/spark:3.5.5

# Switch to root to install system packages
USER root

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install jq
RUN apt-get update && apt-get install -y jq curl

# Create the kafka directory and copy producer.py into the container
COPY producer.py /opt/bitnami/spark/
COPY stream-processor.py /opt/bitnami/spark/
COPY view_data.py /opt/bitnami/spark/

# Create a directory for your data and checkpoints
RUN mkdir -p /opt/bitnami/spark/data /opt/bitnami/spark/checkpoints

# Set the working directory
WORKDIR /opt/bitnami/spark/