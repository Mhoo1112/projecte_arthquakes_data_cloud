# Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# คำสั่งรัน Server ตลอดเวลา
CMD ["uvicorn", "create_api:app_api", "--host", "0.0.0.0", "--port", "8000"]