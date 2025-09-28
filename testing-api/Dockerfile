FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY main.py .
COPY service-account-key.json .
COPY listnames.yml .

# Run the application
CMD ["python", "main.py"]