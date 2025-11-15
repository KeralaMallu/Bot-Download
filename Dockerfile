# Use a modern, slim version of Python as the base image
FROM python:3.11-slim-bookworm

# Set environment variables for Python
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set the working directory
WORKDIR /app

# Copy and install Python packages first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code
COPY . .

# Create a non-root user for better security
RUN useradd --create-home appuser
RUN chown -R appuser:appuser /app
USER appuser

# The command to run when the container starts
CMD ["python", "main.py"]
