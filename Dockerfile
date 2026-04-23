FROM python:3.10-slim

WORKDIR /app

ARG BUILD_DATE
RUN echo "Build date: $BUILD_DATE"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY config.yaml .
COPY src ./src
COPY scripts ./scripts
COPY data ./data

CMD ["python", "main.py"]
