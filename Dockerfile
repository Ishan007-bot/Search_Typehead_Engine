# Search Typeahead System — app image.
# Builds the API, loads the dataset into SQLite, and serves the UI + API.
# Used by docker-compose.yml together with three Redis cache nodes.

FROM python:3.12-slim

WORKDIR /app

# Install deps first (better layer caching).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project.
COPY app/ ./app/
COPY data/ ./data/
COPY ui/ ./ui/
COPY bench/ ./bench/

# Load the dataset into SQLite at build time so the image is ready to run.
RUN python data/load_data.py --reset

EXPOSE 8000

# USE_REDIS / REDIS_HOST / REDIS_PORTS are provided by docker-compose.
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
