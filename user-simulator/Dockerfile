# Dockerfile (VS Code–friendly: bash, optional dev tools, non-root user, tini)
FROM python:3.11-slim

ARG UID=10001
ARG GID=10001
ARG DEV_TOOLS=1  # set to 0 for lean prod image

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HOME=/home/user_simulator \
    PATH=/home/user_simulator/.local/bin:$PATH

# Base + bash + tini; optional dev tools for VS Code attach
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates bash tini \
    && if [ "$DEV_TOOLS" = "1" ]; then apt-get install -y --no-install-recommends \
      git curl less procps iproute2 nano vim sudo; \
      echo "user_simulator ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/90-user_simulator; fi \
    && rm -rf /var/lib/apt/lists/*

# Non-root user for VS Code terminal/shell
RUN groupadd -g $GID user_simulator \
 && useradd -m -u $UID -g $GID -s /bin/bash user_simulator

WORKDIR /app
COPY . .

# Python deps + editable install + pytest (for initContainer preflight)
RUN pip install --upgrade pip setuptools wheel \
 && pip install -r requirements.txt \
 && pip install pytest \
 && pip install -e .

# Permissions for workspace
RUN chown -R user_simulator:user_simulator /app

USER user_simulator

EXPOSE 8000

# Tini = clean signal handling when attaching VS Code terminals
ENTRYPOINT ["/usr/bin/tini","--"]
CMD ["python","-m","user_simulator.app"]

# docker build -t user_simulator:latest .
# docker tag user_simulator:latest awesomeplant/user_simulator:latest
# docker push awesomeplant/user_simulator:latest