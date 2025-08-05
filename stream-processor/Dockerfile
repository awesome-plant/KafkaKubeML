FROM python:3.11-slim
# LABEL org.opencontainers.image.authors="awesome-plant"

WORKDIR /app
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

COPY . . 

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt 
RUN pip install -e .

# docker build -t stream_processor:latest .
# docker tag stream_processor:latest awesomeplant/stream_processor:latest
# docker push awesomeplant/stream_processor:latest

CMD ["python", "consumer.py"]

