FROM python:3.12-slim

WORKDIR /app

# Installera Python-paket
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Kopiera källkod (inkl. listnames.yml om du har den)
COPY . .

# Kör scriptet när jobben startas
CMD ["python", "main.py"]
