# Build stage
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies including PostgreSQL
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install requirements
COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt

# Final stage
FROM python:3.12-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk \
    procps \
    wget \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy wheels from builder stage
COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .

# Install Python packages
RUN pip install --no-cache /wheels/*

# Java environment setup
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$PATH:$JAVA_HOME/bin
ENV PYTHONPATH=/app

# Copy application code
COPY src/ src/
COPY tests/ tests/
COPY data/ data/
COPY conf/ conf/

# CMD ["python", "-m", "src.main"]