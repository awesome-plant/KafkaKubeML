FROM python:3.11-slim
# LABEL org.opencontainers.image.authors="awesome-plant"

WORKDIR /app

COPY . . 
# requirements.txt .
RUN ls -l /app
RUN ls -l /app/src
RUN ls -l /app/src/user-simulator || echo "user_simulator not found"

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt 
RUN pip install -e .

# docker build -t user-simulator:latest .
# awesome-plant/user-simulator:latest

CMD ["python", "-m", "user_simulator.main"]
