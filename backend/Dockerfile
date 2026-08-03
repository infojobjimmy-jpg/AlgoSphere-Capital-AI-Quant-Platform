FROM python:3.12-slim

ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG BUILD_DATE=unknown

LABEL com.algosphere.git.commit="$GIT_COMMIT" \
      com.algosphere.git.branch="$GIT_BRANCH" \
      com.algosphere.build.date="$BUILD_DATE" \
      org.opencontainers.image.revision="$GIT_COMMIT" \
      org.opencontainers.image.source="https://github.com/infojobjimmy-jpg/AlgoSphere-Capital-AI-Quant-Platform"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY config ./config

ENV PYTHONUNBUFFERED=1
