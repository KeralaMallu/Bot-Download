# Use a modern, slim version of Python as the base image
[cite_start]FROM python:3.11-slim-bookworm [cite: 2]

# Set environment variables for Python
[cite_start]ENV PYTHONUNBUFFERED=1 [cite: 2]
[cite_start]ENV PYTHONDONTWRITEBYTECODE=1 [cite: 2]

# Set the working directory
[cite_start]WORKDIR /app [cite: 2]

# Copy and install Python packages first to leverage Docker cache
[cite_start]COPY requirements.txt . [cite: 3]
RUN pip install --no-cache-dir --upgrade pip && \
    [cite_start]pip install --no-cache-dir -r requirements.txt [cite: 3]

# Copy the rest of the application's code (e.g., main.py)
COPY . [cite_start]. [cite: 4]

# Create a non-root user for better security
[cite_start]RUN useradd --create-home appuser [cite: 4]
[cite_start]RUN chown -R appuser:appuser /app [cite: 4]
[cite_start]USER appuser [cite: 4]

# The command to run when the container starts
[cite_start]CMD ["python", "main.py"] [cite: 4]
