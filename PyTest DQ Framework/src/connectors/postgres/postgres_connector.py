import psycopg2

class PostgresConnectorContextManager:
    def __init__(self, db_user, db_password, db_host, db_name, db_port):
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_name = db_name
        self.db_port = db_port
        self.connection = None

    def __enter__(self):
        self.connection = psycopg2.connect(
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
            dbname=self.db_name
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()

    def get_data_sql(self, query):
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            # Возвращаем список словарей для удобства
            return [dict(zip(columns, row)) for row in rows]