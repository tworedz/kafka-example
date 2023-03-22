import uuid

import aiokafka
import aioredis
from fastapi import FastAPI
from pydantic import BaseModel

try:
    import orjson as json
except ImportError:
    import json


app = FastAPI()
redis = aioredis.Redis()
producer = aiokafka.AIOKafkaProducer(bootstrap_servers="localhost:29092")


@app.on_event("startup")
async def startup() -> None:
    await producer.start()


@app.on_event("shutdown")
async def shutdown() -> None:
    await producer.stop()


class RequestSchema(BaseModel):
    search: str
    request_id: uuid.UUID


@app.get("/")
async def start_request(search: str) -> uuid.UUID:
    request_id = uuid.uuid4()
    request_schema = RequestSchema(search=search, request_id=request_id)
    await producer.send_and_wait("parser", request_schema.json().encode())
    return request_id


class Response(BaseModel):
    data: dict


@app.get("/check_status/", response_model=list[Response])
async def check_status(request_id: uuid.UUID):
    values = await redis.lrange(str(request_id), 0, -1)
    return [Response.parse_raw(obj) for obj in values]
