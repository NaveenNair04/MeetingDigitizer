FROM python:3.10-slim

WORKDIR /app

# Install system dependencies for audio
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libsndfile1 \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Install system deps + git
RUN apt-get update && apt-get install -y git build-essential && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

COPY audio-consumer.py .

CMD ["python", "audio-consumer.py"]

