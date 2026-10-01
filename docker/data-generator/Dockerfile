FROM python:3.12.12-slim

# Print logs immediately (so `docker logs data-generator` shows each action live)
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generator.py .

# Run as a normal user, not root
RUN useradd --create-home generator
USER generator

CMD ["python", "generator.py"]
