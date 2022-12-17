try:
    import pymssql
except ImportError:
    raise ImportError("pymysql is required for mssql support.")


class MSSQLConnection:
    def __init__(self, host: str, user: str, password: str, db_name: str):
        self.__host = host
        self.__user = user
        self.__password = password
        self.__db_name = db_name
        self.connection = None

    def __enter__(self):
        self.connection = self.__connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__disconnect()

    def __connect(self):
        return pymssql.connect(self.__host, self.__user, self.__password, self.__db_name)

    def __disconnect(self):
        if self.connection:
            self.connection.close()
