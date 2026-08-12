FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY pyproject.toml README.md ./
RUN python -c "import subprocess, sys, tomllib; config = tomllib.load(open('pyproject.toml', 'rb')); subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--no-cache-dir', *config['project']['dependencies']])"
RUN pip install --no-cache-dir "setuptools>=69"

COPY src ./src
COPY config ./config
COPY dashboard ./dashboard
COPY alembic.ini ./
COPY migrations ./migrations

RUN pip install --no-cache-dir --no-deps --no-build-isolation .
COPY scripts ./scripts

EXPOSE 8000
CMD ["uvicorn", "funding_arbitrage.main:app", "--host", "0.0.0.0", "--port", "8000"]
