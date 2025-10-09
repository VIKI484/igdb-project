FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy remaining files
COPY main.py .
COPY value_config.yml .

# Run script
CMD [ "python", "main.py" ]