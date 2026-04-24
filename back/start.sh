#!/bin/sh
echo "Starting application..."

# Fetch secrets from Google Secret Manager
echo "Fetching secrets..."
poetry run python fetch_secrets.py || { echo "Failed to fetch secrets"; exit 1; }

# Start Langchain server
echo "Starting Langchain server..."
exec poetry run langchain serve --host 0.0.0.0 --port=8080
