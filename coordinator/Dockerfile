# Build from the repository root:  docker build -f coordinator/Dockerfile -t erms/coordinator:latest .
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY coordinator ./coordinator
EXPOSE 8000
CMD ["uvicorn", "coordinator.app:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
