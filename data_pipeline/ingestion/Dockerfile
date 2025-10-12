FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY main.py .
COPY value_config.yml .

ENV PORT=8080
EXPOSE 8080

# Run the application
CMD ["python", "main.py"]