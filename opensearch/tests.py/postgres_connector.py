import psycopg
import time
import os
import asyncio


class PostgresConnector:

    def __init__(self, db_name: str = 'account_db') -> None:
        self.conn = None

    async def set_connection(self, db_name: str = 'account_db'):
        if not self.conn:
            self.db_name = db_name
            self.user = 'isand'
            self.password = 'sf3dvxQFWq@!'

            if os.getenv("IN_DOCKER_CONTAINER"):
                self.host = '193.232.208.58'
            else:
                self.host = 'localhost'
            self.port = '5432'
            while True:
                try:
                    self.conn = await psycopg.AsyncConnection.connect(dbname=self.db_name, user=self.user,
                                                                      password=self.password, host=self.host, port=self.port)
                    break
                except:
                    print("Can't connect to postgres")
                    time.sleep(5)

    async def get_cursor(self):
        await self.set_connection()
        self.cur = self.conn.cursor()
        return self.cur

    def close_connection(self):
        self.cur.close()
        if self.conn:
            self.conn.close()


async def main():
    try:
        request = {
            "id"
            "p_title": "",
            "p_annotation": "",
            "authors": [],
            "p_type": str
        }
        cursor = await PostgresConnector().get_cursor()
        sql = """
            SELECT p.id, p.title, pt.name FROM publications p
            LEFT JOIN publication_type pt on pt.id = p.publication_type_id
            WHERE p.id = 12
        """
        fields = ["p_title", "p_annotation", "authors"]
        await cursor.execute(sql)
        sql_request = await cursor.fetchone()
        if sql_request:
            request["id"] = sql_request[0]
            request["p_title"] = sql_request[1]
            request["p_type"] = sql_request[2]

    except Exception as e:
        print(e)
        return {}
asyncio.run(main=main())
