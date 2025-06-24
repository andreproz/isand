import psycopg
import time
import os


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
