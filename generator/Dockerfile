FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY transaction_generator.py .

CMD ["python", "transaction_generator.py"]
