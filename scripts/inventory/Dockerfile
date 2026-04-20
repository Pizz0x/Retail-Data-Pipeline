# light python image for inventory service
FROM python:3.10-slim

# set work directory
WORKDIR /app

# install dependencies, layer caching for faster builds (only re-run if requirements.txt changes)
COPY inventory_requirements.txt .
RUN pip install --no-cache-dir -r inventory_requirements.txt

COPY . .
CMD ["python", "inventory_processor.py"]