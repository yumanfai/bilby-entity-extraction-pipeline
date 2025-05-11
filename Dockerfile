FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/extract_entities.py .

# Pre-download the GLiNER model
RUN python -c "from gliner import GLiNER; GLiNER.from_pretrained('urchade/gliner_multi-v2.1')"

CMD ["python", "extract_entities.py", "/app/input.txt", "/app/output.json"]