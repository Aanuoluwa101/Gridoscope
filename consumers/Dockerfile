# Build from the repo root so this can COPY consumers/ as a unit:
#   docker build -f consumers/Dockerfile -t gridoscope-consumer .
FROM python:3.11-slim

WORKDIR /app

# build-essential + libsnappy-dev are needed to compile python-snappy's C extension
# (the consumer decompresses snappy-compressed messages the producer sends).
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

COPY consumers/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumers/ consumers/

CMD ["python", "consumers/runner.py"]
