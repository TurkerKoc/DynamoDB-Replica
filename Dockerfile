FROM python:3.9-slim as base
WORKDIR /app
COPY . .

FROM base as kv_client
ENTRYPOINT ["python", "kv_client.py"]

FROM base as kv_server
ENTRYPOINT ["python", "kv_server.py"]

FROM base as ecs_server
ENTRYPOINT ["python", "ecs-server.py"]
