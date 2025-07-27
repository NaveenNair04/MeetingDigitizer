FROM bitnami/kafka:latest

# Temporarily switch to root to install netcat
USER root

RUN apt-get update && \
    apt-get install -y netcat-traditional && \
    apt-get clean

# Copy and make your script executable
COPY --chmod=755 kafka-setup.sh /kafka-setup.sh

# ✅ Switch back to the default user expected by Bitnami (typically UID 1001)
USER 1001

