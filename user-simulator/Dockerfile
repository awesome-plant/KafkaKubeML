FROM python:3.11-slim
# LABEL org.opencontainers.image.authors="awesome-plant"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# docker build -t kafkakubeml:user_simulator .

# CMD ["python", "generate.py"]
