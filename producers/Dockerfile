# Build from the repo root so this can COPY producers/ as a unit:
#   docker build -f producers/Dockerfile -t gridoscope-producer .
FROM python:3.11-slim

WORKDIR /app

# build-essential + libsnappy-dev are needed to compile python-snappy's C extension.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

COPY producers/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producers/ producers/

CMD ["python", "producers/engine.py"]
# CMD ["python", "producers/dry_run.py --meters 5 --speed 10"]
