FROM python:3.11-slim
# LABEL org.opencontainers.image.authors="awesome-plant"

WORKDIR /app

COPY . . 
# requirements.txt .
RUN ls -l /app
RUN ls -l /app/src
RUN ls -l /app/src/user_simulator || echo "user_simulator not found"

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt 
RUN pip install -e .

# docker build -t user_simulator:latest .
# docker tag user_simulator:latest awesomeplant/user_simulator:latest
# docker push awesomeplant/user_simulator:latest
# awesomeplant/user_simulator:latest

CMD ["python", "-m", "user_simulator.main"]

