# Use the official lightweight Python image
FROM python:3.11-slim

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app
COPY app.py .

# Expose the port Render will use
EXPOSE 5000

# Run the Flask app
CMD ["python", "app.py"]
