FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY config ./config
COPY dashboard ./dashboard
COPY alembic.ini ./
COPY migrations ./migrations

RUN pip install --no-cache-dir .

EXPOSE 8000
CMD ["uvicorn", "funding_arbitrage.main:app", "--host", "0.0.0.0", "--port", "8000"]
