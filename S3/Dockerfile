FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and Makefile first (for better layer caching)
COPY requirements.txt Makefile ./

# Install Python dependencies
RUN make install

# Copy the rest of the application
COPY . .

# Optional: Run formatting and linting during build
# RUN make refactor

CMD ["python", "s3_upload.py"]