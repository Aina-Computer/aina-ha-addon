FROM python:3.12-alpine

WORKDIR /app
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ .

VOLUME ["/data"]
CMD ["python3", "main.py"]
