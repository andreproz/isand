from opensearchpy import AsyncOpenSearch
import asyncio
from dotenv import load_dotenv
import os

opensearch_client: AsyncOpenSearch


class OpenSearchConnector:

    @staticmethod
    async def connect_and_init_opensearch():
        global opensearch_client
        load_dotenv()
        hosts = os.getenv("OPENSEARCH_DEBUG_URI").split(',')
        if os.getenv("IN_DOCKER_CONTAINER"):
            hosts = os.getenv("OPENSEARCH_DOCKER_URI").split(',')
        login = os.getenv("OPENSEARCH_ISAND_LOGIN")
        password = os.getenv("OPENSEARCH_ISAND_PASSWORD")
        auth = (login, password)
        ca_certs_path = "../opensearch_config_files/certs/root-ca.pem"
        while True:
            try:
                opensearch_client = AsyncOpenSearch(
                    hosts=hosts,
                    http_compress=True,
                    http_auth=auth,
                    use_ssl=True,
                    verify_certs=False,
                    ssl_assert_hostname=False,
                    ssl_show_warn=False,
                    ca_certs=ca_certs_path
                )
                if await opensearch_client.info():
                    print(f'Connected to opensearch with uri {hosts}')
                    break
                else:
                    raise Exception(
                        f"Not connected to opensearch with uri {hosts}")
            except Exception as ex:
                print(f'Cant connect to opensearch: {ex}')
                # Подождать 5 секунд перед следующей попыткой подключения
                await asyncio.sleep(5)

    @staticmethod
    def get_opensearch_client() -> AsyncOpenSearch:
        global opensearch_client
        return opensearch_client

    @staticmethod
    async def close_opensearch_connect():
        global opensearch_client
        if opensearch_client is None:
            return
        await opensearch_client.close()


# async def main():
#     await OpenSearchConnector.connect_and_init_opensearch()
#     await OpenSearchConnector.close_opensearch_connect()

# asyncio.run(main=main())
