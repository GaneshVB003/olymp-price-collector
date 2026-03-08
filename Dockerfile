FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY app.py .

# Create persistent data directories
RUN mkdir -p data/ticks data/candles data/orderbook \
             data/sentiment data/predictions data/sessions

EXPOSE 5000

CMD ["gunicorn", "app:app", \
     "--workers", "2", \
     "--threads", "4", \
     "--timeout", "60", \
     "--bind", "0.0.0.0:5000", \
     "--log-level", "info"]
