FROM python:3.11-slim

# Set working directory and copy requirements text file
WORKDIR /app
COPY /pipeline/dependencies.txt .

# Install Python and pip
RUN apt-get update && apt-get install -y python3 python3-pip curl

# Install required Python packages
#RUN pip3 install pandas nba_api sqlalchemy psycopg2-binary
COPY ../.env .
RUN pip3 install --no-cache-dir -r dependencies.txt

# Copy pipeline package
COPY ../setup.py .
COPY /pipeline ./pipeline

# Install your package in editable mode
RUN pip install -e .