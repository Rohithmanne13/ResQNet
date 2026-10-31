# Build from the repository root:  docker build -f matching_service/Dockerfile -t erms/matching-service:latest .
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY matching_service ./matching_service
EXPOSE 8000
CMD ["uvicorn", "matching_service.app:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
