FROM python:3.12-slim

WORKDIR /app

ENV HOST 0.0.0.0
ENV NODE_ENV=production HOSTNAME="0.0.0.0" NEXT_TELEMETRY_DISABLED=1

# Install curl and git, then clean up apt cache to reduce image size
RUN apt-get update && \
    apt-get install -y curl git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml /app/
RUN pip install poetry && poetry install --no-root

COPY . /app
RUN chmod +x /app/start.sh

EXPOSE 8080
ENTRYPOINT ["/app/start.sh"]
