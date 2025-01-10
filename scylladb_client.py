from datetime import datetime, timedelta, time

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

from datetime import datetime

class ScyllaDBClient:
    def __init__(self, hosts, keyspace):
        """
        Инициализация клиента ScyllaDB.
        :param hosts: Список хостов для подключения.
        :param keyspace: Название keyspace в ScyllaDB.
        """
        self.cluster = Cluster(hosts)
        self.session = self.cluster.connect()
        self.keyspace = keyspace
        self._setup_keyspace_and_table()

    def _setup_keyspace_and_table(self):
        """
        Создание keyspace и таблицы, если они отсутствуют.
        """
        self.session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
        """)
        self.session.set_keyspace(self.keyspace)

        self.session.execute("""
        CREATE TABLE IF NOT EXISTS activity (
            username TEXT,
            creation_date TIMESTAMP,
            id UUID,
            status TEXT,
            PRIMARY KEY (username, creation_date)
        ) WITH CLUSTERING ORDER BY (creation_date DESC);
        """)
        
        self.session.execute("""
        CREATE TABLE IF NOT EXISTS daily_report (
            username TEXT,
            creation_date TIMESTAMP,
            id UUID,
            total TIME,
            most_visited_hour TIME,
            PRIMARY KEY (username, creation_date)
        )
        """
        )

    def create_activity(self, username, status):
        """
        Создает документ в базе данных.
        :param username: Имя пользователя.
        :param status: Статус пользователя.
        """
        creation_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query = """
        INSERT INTO activity (id, username, creation_date, status)
        VALUES (uuid(), %s, %s, %s)
        """
        self.session.execute(query, (username, creation_date, status))
        print(f"Документ для пользователя '{username}' успешно создан.")

    def get_activity(
        self,
        username: str = None,
        status: str = None,
        creation_date_start: str = None,
        creation_date_end: str = None
    ):
        """
        Получает документы из базы данных с возможностью фильтрации.
        
        :param username: Фильтр по имени пользователя.
        :param status: Фильтр по статусу.
        :param creation_date_start: Начальная дата для фильтрации.
        :param creation_date_end: Конечная дата для фильтрации.
        :return: Список документов, соответствующих фильтрам.
        """
        query = "SELECT * FROM activity"
        conditions = []
        parameters = []

        if username:
            conditions.append("username = %s")
            parameters.append(username)

        if status:
            conditions.append("status = %s")
            parameters.append(status)

        if creation_date_start and creation_date_end:
            conditions.append("creation_date >= %s AND creation_date <= %s")
            parameters.extend([creation_date_start, creation_date_end])
        elif creation_date_start:
            conditions.append("creation_date >= %s")
            parameters.append(creation_date_start)
        elif creation_date_end:
            conditions.append("creation_date <= %s")
            parameters.append(creation_date_end)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ALLOW FILTERING"

        try:
            rows = self.session.execute(query, tuple(parameters))
            results = [
                {
                    "id": str(row.id),
                    "username": row.username,
                    "creation_date": row.creation_date.isoformat(),
                    "status": row.status
                }
                for row in rows
            ]
            return {"documents": results}
        except Exception as e:
            raise RuntimeError(f"Ошибка выполнения запроса: {str(e)}")

    def get_last_activity(self, username: str):
        """
        Возвращает последнюю запись (по дате) для заданного username.
        """
        # Запрос с сортировкой по creation_date в убывающем порядке и лимитом в 1
        # Важно: используем ALLOW FILTERING, т.к. creation_date не в clustering key.
        query_str = """
            SELECT id, username, creation_date, status
            FROM activity
            WHERE username = %s
            LIMIT 1
        """
        statement = SimpleStatement(query_str)
        rows = self.session.execute(statement, (username,))

        # Преобразуем результат в список (в нашем случае из одной записи)
        results = [
            {
                "id": str(row.id),
                "username": row.username,
                "creation_date": row.creation_date.isoformat(),
                "status": row.status
            }
            for row in rows
        ]

        # Либо вернуть список, либо вернуть словарь одной записи
        # (если нужно только 1 запись, можно вернуть results[0] или None)
        return {"documents": results}

    def create_daily_report(self, username, total : int):
        """
        Создает документ в базе данных.
        :param username: Имя пользователя.
        :param status: Статус пользователя.
        """
        creation_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query = """
        INSERT INTO activity (id, username, creation_date, status)
        VALUES (uuid(), %s, %s, %s)
        """
        self.session.execute(query, (username, creation_date))
        print(f"Документ для пользователя '{username}' успешно создан.")

    def upsert_daily_report(self, username, most_visited_hour, total):
            """
            Обновляет или создаёт запись в таблице daily_report.
            
            :param username: Имя пользователя.
            :param most_visited_hour: Время, которое нужно записать в most_visited_hour (тип TIME).
            :param total: Общее количество (тип TIME).
            """
            # Убедимся, что most_visited_hour и total в правильном формате (строка или datetime.time)
            if isinstance(most_visited_hour, str):
                # Преобразование строки формата "HH:MM:SS" в объект time
                most_visited_hour = datetime.strptime(most_visited_hour, "%H:%M:%S").time()
            elif not isinstance(most_visited_hour, time):
                raise ValueError("most_visited_hour должен быть строкой 'HH:MM:SS' или объектом datetime.time")

            if isinstance(total, str):
                # Преобразование строки формата "HH:MM:SS" в объект time
                total = datetime.strptime(total, "%H:%M:%S").time()
            elif not isinstance(total, time):
                raise ValueError("total должен быть строкой 'HH:MM:SS' или объектом datetime.time")

            
            # Определяем начало и конец текущего дня
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            today_end = today_start + timedelta(days=1)

            # Проверяем, существует ли запись для пользователя за текущий день
            select_query = """
                SELECT creation_date 
                FROM daily_report
                WHERE username = %s AND creation_date >= %s AND creation_date < %s
            """
            rows = self.session.execute(select_query, (username, today_start, today_end))

            if rows:
                # Если запись существует, обновляем её
                update_query = """
                    UPDATE daily_report
                    SET most_visited_hour = %s, total = %s
                    WHERE username = %s AND creation_date = %s
                """
                existing_record = rows[0]
                self.session.execute(update_query, (most_visited_hour, total, username, existing_record.creation_date))
                print(f"Обновлена запись для пользователя '{username}' на дату {existing_record.creation_date}.")
            else:
                # Если записи нет, создаём новую
                insert_query = """
                    INSERT INTO daily_report (username, creation_date, id, most_visited_hour, total)
                    VALUES (%s, %s, uuid(), %s, %s)
                """
                self.session.execute(insert_query, (username, today_start, most_visited_hour, total))
                print(f"Создана новая запись для пользователя '{username}' на текущие сутки.")

    def get_daily_report(self, username, most_visited_hour = None, total = None, creation_date_start = None, creation_date_end = None):
        """
            Получает данные из таблицы daily_report.
            
            :param username: Имя пользователя.
            :param most_visited_hour: Время, которое нужно записать в most_visited_hour (тип TIME).
            :param total: Общее количество (тип TIME).
            :param creation_date_start: Начальная дата от сортироваки (тип TIME)
            :param creation_date_end: Конечная дата от сортироваки (тип TIME)
        """
        query = "SELECT * FROM daily_report"
        conditions = []
        parameters = []

        if username:
            conditions.append("username = %s")
            parameters.append(username)
            
        if most_visited_hour:
            conditions.append("most_visited_hour = %s")
            parameters.append(most_visited_hour)
            
        if total:
            conditions.append("total = %s")
            parameters.append(total)

        if creation_date_start and creation_date_end:
            conditions.append("creation_date >= %s AND creation_date <= %s")
            parameters.extend([creation_date_start, creation_date_end])
        elif creation_date_start:
            conditions.append("creation_date >= %s")
            parameters.append(creation_date_start)
        elif creation_date_end:
            conditions.append("creation_date <= %s")
            parameters.append(creation_date_end)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ALLOW FILTERING"

        try:
            rows = self.session.execute(query, tuple(parameters))
            results = [
                {
                    "id": str(row.id),
                    "username": row.username,
                    "creation_date": row.creation_date.isoformat(),
                    "most_visited_hour": str(row.most_visited_hour).split('.')[0] if row.most_visited_hour else None,
                    "total": str(row.total).split('.')[0] if row.total else None
                }
                for row in rows
            ]
            return {"documents": results}
        except Exception as e:
            raise RuntimeError(f"Ошибка выполнения запроса: {str(e)}")



    def close(self):
        """
        Закрывает соединение с базой данных.
        """
        self.cluster.shutdown()
