# ===============================================
# 🐳 Dockerfile — Meeting Summarizer Service
# ===============================================

FROM python:3.10-slim

WORKDIR /app
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy summarizer code and model
COPY summarizer.py /app/summarizer.py
COPY pegasus_custom /app/pegasus_custom

# Create output directory inside container
RUN mkdir -p /app/output

# Default command: run summarizer
CMD ["python", "summarizer.py"]
