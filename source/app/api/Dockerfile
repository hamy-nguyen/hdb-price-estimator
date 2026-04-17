FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ ./backend/

RUN python -c "\
from huggingface_hub import hf_hub_download; \
hf_hub_download(repo_id='hamynguyen/hdb-price-estimator-model', filename='model.pkl', local_dir='/app/models')"

ENV MODEL_PATH=/app/models/model.pkl

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:7860/health')"

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "7860"]
