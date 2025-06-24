import asyncio
from db_connectors.opensearch_connector import OpenSearchConnector


async def startup():
    await OpenSearchConnector.connect_and_init_opensearch()


async def shutdown():
    await OpenSearchConnector.close_opensearch_connect()
