import asyncio

import httpx

try:
    import orjson as json
except ImportError:
    import json

import random
from typing import AsyncIterator

from parsing._main import app
from parsing._main import redis
from parsing._main import register
from parsing.parser import BaseParser

TOPIC = "parser-1"

print(f"LOADED {__name__}")


@register
class Example1Parser(BaseParser):
    topic = TOPIC

    @classmethod
    async def parse(cls, search: str) -> AsyncIterator:
        to_value = random.randrange(10, 20)
        sleep = random.randrange(200, 2000) / 1000
        for i in range(to_value):
            yield {
                "data": {
                    "total": to_value,
                    "result": i,
                    "message": f"from parser 1 searching for {search}",
                }
            }
            await asyncio.sleep(sleep + 5)


client = httpx.AsyncClient()


@app.agent(TOPIC)
async def topic_handler(requests):
    async for request_id, request in requests.items():
        async for item in Example1Parser.parse(request.search):
            await redis.rpush(request_id, json.dumps(item))
            response = await client.post(f"http://127.0.0.1/pubsub/{request_id.decode()}", json=item, timeout=2)
            print(response.status_code)
        await client.post(f"http://127.0.0.1/pubsub/{request_id.decode()}", content="that's all", timeout=2)
