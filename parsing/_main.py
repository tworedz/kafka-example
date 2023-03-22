import asyncio
import logging
import pkgutil
import uuid
from typing import AsyncIterator

try:
    import orjson as json
except ImportError:
    import json

import aioredis
import faust

from . import parsers


class RequestParams(faust.Record):
    search: str
    request_id: uuid.UUID


app = faust.App("parser", broker="kafka://127.0.0.1:29092")
redis = aioredis.Redis()
logger = logging.getLogger(__name__)

RequestTopic = app.topic("parser", value_type=RequestParams)


PARSERS: list[str] = []


def register(cls):
    PARSERS.append(cls.topic)
    return cls


def register_parsers() -> None:
    parsers_path = parsers.__path__
    mod_infos = pkgutil.walk_packages(parsers_path, f"{parsers.__name__}.")
    for _, name, _ in mod_infos:
        __import__(name, fromlist=["_trash"])


register_parsers()


@app.task
async def declare_topics() -> None:
    logger.warning("DECLARING")
    await RequestTopic.maybe_declare()


@app.agent(RequestTopic)
async def request_topic_handler(requests: AsyncIterator[RequestParams]):
    logger.warning("starting handler")
    async for request in requests:
        tasks = [
            asyncio.create_task(
                app.producer.send(
                    parser,
                    str(request.request_id).encode(),
                    json.dumps(request.dumps()),
                    None,
                    None,
                    None,
                )
            )
            for parser in PARSERS
        ]
        await asyncio.gather(*tasks)
        logger.warning("Got request: %s: %s", request.request_id, request.search)


if __name__ == "__main__":
    app.main()
