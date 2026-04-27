FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY config.py .
COPY data/ data/
COPY ai/ ai/
COPY api/ api/
COPY services/ services/
COPY communication/ communication/
COPY scripts/ scripts/
COPY models.py .
COPY start.py .

RUN pip install --no-cache-dir .

# Generate synthetic data then train both models
RUN python -m data.generate_sample_data
RUN python -m scripts.train_models

EXPOSE 8000
CMD ["python", "start.py"]
