# stream-processor/Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# OS deps (ca-certificates only)
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 1) Install runtime deps first (layer cache)
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 2) Install our package in editable mode for clean imports in both runtime & tests
COPY pyproject.toml ./pyproject.toml
COPY src ./src
RUN pip install -e .

# 3) (optional) include tests in the image so initContainers can run them
COPY tests ./tests

# Non-root user
ARG user_name=stream_processor
ARG UID=10001
ARG GID=10001
RUN groupadd -g ${GID} ${user_name} \
 && useradd -m -u ${UID} -g ${GID} -s /bin/bash ${user_name} \
 && chown -R ${user_name}:${user_name} /app
USER ${user_name}
ENV HOME=/home/${user_name}

# Helpful for ad-hoc runs, and extra safety for imports
ENV PYTHONPATH=/app/src

# Run service
CMD ["python","-m","stream_processor.app"]


# docker build -t stream_processor:latest .
# docker tag stream_processor:latest awesomeplant/stream_processor:latest
# docker push awesomeplant/stream_processor:latest
