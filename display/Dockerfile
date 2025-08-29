FROM python:3.10-slim

WORKDIR /app

# Install system dependencies for Tkinter + GUI + fonts
RUN apt-get update && apt-get install -y \
    python3-tk \
    tk-dev \
    libjpeg-dev \
    zlib1g-dev \
    libgl1 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY . .

# Run the GUI
CMD ["python", "display.py"]
