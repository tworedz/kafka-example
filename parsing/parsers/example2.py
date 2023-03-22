import asyncio

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

TOPIC = "parser-2"


print(f"LOADED {__name__}")


@register
class Example2Parser(BaseParser):
    topic = TOPIC

    @classmethod
    async def parse(cls, search: str) -> AsyncIterator:
        to_value = 5
        sleep = random.randrange(200, 2000) / 1000
        for i in range(to_value):
            yield {
                "data": {
                    "total": to_value,
                    "result": i,
                    "message": f"from parser 2 searching for {search} with another field",
                    "extra": sleep,
                }
            }
            await asyncio.sleep(sleep)


@app.agent(TOPIC)
async def topic_handler(requests):
    async for request_id, request in requests.items():
        async for item in Example2Parser.parse(request.search):
            await redis.rpush(request_id, json.dumps(item))
            print("PARSER 2 pushed")
