# -----------------------------
# Base image with Python
# -----------------------------
FROM python:3.12-slim

# -----------------------------
# Set environment variables
# -----------------------------
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /home/amine/first_project

# -----------------------------
# Install dependencies
# -----------------------------
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    gcc \
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# -----------------------------
# Copy requirements and install
# -----------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# -----------------------------
# Copy ETL script and API folder
# -----------------------------
COPY etl.py .
RUN mkdir -p api_data

# -----------------------------
# Set default command
# -----------------------------
CMD ["python3", "etl.py"]
