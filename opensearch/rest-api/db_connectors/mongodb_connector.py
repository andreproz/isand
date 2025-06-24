from typing import List, Mapping, Any, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os

if os.getenv("IN_DOCKER_CONTAINER"):
    MONGODB_URL = "mongodb://root:example@193.232.208.58:27017"
else:
    MONGODB_URL = "mongodb://root:example@localhost:27017"

MONGODB_DB_NAME = "isand"
MONGODB_COLLECTION_NAME = "publs"


class MongoDBConnection:
    _client: Optional[AsyncIOMotorClient] = None

    @classmethod
    async def connect(cls) -> AsyncIOMotorClient:
        if cls._client is None:
            cls._client = AsyncIOMotorClient(MONGODB_URL)
        return cls._client

    @classmethod
    async def get_db(cls):
        await MongoDBConnection.connect()
        db = cls._client[MONGODB_DB_NAME]
        return db

    @classmethod
    async def get_collection(cls, collection_name: str):
        db = await MongoDBConnection.get_db()
        collection = db[collection_name]
        return collection

    @classmethod
    async def close(cls):
        if cls._client:
            cls._client.close()
            cls._client = None
