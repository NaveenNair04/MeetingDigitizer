FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies for OpenCV, ffmpeg, and fonts
RUN apt-get update && apt-get install -y \
    libgl1 libglib2.0-0 ffmpeg build-essential \
    python3-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the consumer code
COPY . .

# Run the diagram consumer
CMD ["python", "diagram_consumer.py"]
