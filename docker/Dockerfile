FROM python:3.11-slim
WORKDIR /app
COPY src ./src
COPY scripts ./scripts
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "scripts/run_node.py"]
