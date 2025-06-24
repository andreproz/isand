import psycopg2


class PostgresConnector:

    def __init__(self) -> None:
        self.dbname = 'isandDB'
        self.user = 'isand'
        self.password = 'sf3dvxQFWq@!'
        self.host = '193.232.208.58'
        self.port = '5432'

    # Подключение к базе данных
    def get_cursor(self) -> psycopg2.extensions.cursor:
        self.conn = psycopg2.connect(dbname=self.dbname, user=self.user,
                                     password=self.password, host=self.host, port=self.port)

        # Создание курсора для выполнения SQL-запросов
        self.cur = self.conn.cursor()
        return self.cur

    # Закрытие курсора и соединенияЭ
    def close_connection(self):
        self.cur.close()
        self.conn.close()
