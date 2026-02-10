FROM python:3.9-slim

# Install system dependencies including Java and Spark requirements
RUN apt-get update && apt-get install -y --no-install-recommends \
    make \
    # Updated from 11 to 17
    openjdk-17-jre-headless \
    # Required by Spark for process management
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set Java home environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /app

# Copy requirements and Makefile first (for better layer caching)
COPY requirements.txt Makefile ./

# Install Python dependencies including PySpark with S3 support
RUN pip install --no-cache-dir -r requirements.txt pyspark[hadoop,aws]

# Copy the rest of the application
COPY . .

CMD ["python", "compute.py"]