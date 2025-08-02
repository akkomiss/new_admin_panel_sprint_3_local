from state import State, RedisStorage

class PostgresProducer:
    """
    Класс для пакетного извлечения данных из таблицы Postgres с поддержкой состояния через State.
    """
    def __init__(self, pg_conn, state: State, table: str, batch_size=100):
        """
        :param pg_conn: Открытое соединение с Postgres
        :param state: Экземпляр State (работает с RedisStorage)
        :param table: Имя таблицы (str)
        :param batch_size: Размер пачки (int)
        """
        self.pg_conn = pg_conn
        self.state = state
        self.table = table
        self.batch_size = batch_size

    def fetch_batch(self):
            """Извлечь пачку новых записей."""
            last_updated_at = self.state.get_state('last_updated_at') or '1970-01-01T00:00:00'
            last_id = self.state.get_state('last_id') or '00000000-0000-0000-0000-000000000000'
            
            # Для первого запуска пробуем обычную схему:
            query = """
                SELECT id, updated_at
                FROM content.film_work
                WHERE (updated_at > %s)
                OR (updated_at = %s AND id > %s)
                ORDER BY updated_at, id
                LIMIT %s;
            """
            params = (last_updated_at, last_updated_at, last_id, self.batch_size)
            with self.pg_conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

            # Если все updated_at в пачке одинаковые и совпадают с last_updated_at — переключаемся только на id
            if rows and all(r[1] == last_updated_at for r in rows): # возвращает True если значение поля updated_at совпадает с текущим курсором last_updated_at
                query = """
                    SELECT id, updated_at
                    FROM content.film_work
                    WHERE id > %s
                    ORDER BY id
                    LIMIT %s;
                """
                params = (last_id, self.batch_size)
                with self.pg_conn.cursor() as cur:
                    cur.execute(query, params)
                    rows = cur.fetchall()

            return rows
            
    def extract(self, redis_conn, redis_queue_name):
        """
        Основной метод: извлекает новые id и пишет их в очередь/set в Redis.
        После успешной обработки пачки обновляет состояние.
        """
        rows = self.fetch_batch()
        if not rows:
            print(f'Нет новых данных в таблице {self.table}')
            return

        # Пишем id в Redis (set или очередь, по ситуации)
        for row in rows:
            redis_conn.sadd(redis_queue_name, str(row[0]))  # uuid в строку
        
        print(f'Обработано {len(rows)} записей из {self.table}')
        return rows
  
        
