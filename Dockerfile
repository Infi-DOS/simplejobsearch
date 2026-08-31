FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JOBSEARCH_DB_PATH=/app/data/jobs.db

WORKDIR /app
COPY requirements.txt pyproject.toml README.md ./
COPY src ./src
COPY migrations ./migrations
COPY *.py ./
RUN pip install --no-cache-dir .

RUN mkdir -p /app/data
EXPOSE 5000
CMD ["jobsimplesearch", "web"]

