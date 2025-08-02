from state import State, RedisStorage

class PostgresEnricher:
    """
    Класс для обогащения данных: берет id из Redis, находит связанные film_work в Postgres.
    """
    def __init__(self, pg_conn, state: State, source_queue: str, relation_field: str, batch_size=100):
        """
        :param pg_conn: Открытое соединение с Postgres
        :param state: Экземпляр State (работает с RedisStorage)
        :param source_queue: Имя очереди/id set в Redis (например, 'person_ids')
        :param relation_field: Поле связи ('person_id' или 'genre_id')
        :param batch_size: Размер пачки обработки
        """
        self.pg_conn = pg_conn
        self.state = state
        self.source_queue = source_queue
        self.relation_field = relation_field
        self.batch_size = batch_size

    def fetch_batch(self, redis_conn):
        """
        Забирает пачку id из Redis (set или list), чтобы обработать только небольшое количество за раз.
        """
        # Забираем пачку id (например, sscan для set, или lrange/lpop для list)
        ids = redis_conn.srandmember(self.source_queue, self.batch_size)
        ids = [i.decode() if isinstance(i, bytes) else str(i) for i in ids]
        return ids

    def extract(self, redis_conn, target_queue_name):
        """
        Основной метод: берет пачку id из Redis, ищет связанные film_work и пишет их id в target_queue (Redis).
        """
        source_ids = self.fetch_batch(redis_conn)
        if not source_ids:
            print(f'Нет новых id для обогащения из {self.source_queue}')
            return []

        # Генерируем нужный SQL под тип связи:
        if self.relation_field == 'person_id':
            query = """
                SELECT fw.id
                FROM content.film_work fw
                JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                WHERE pfw.person_id = ANY(%s)
            """
        elif self.relation_field == 'genre_id':
            query = """
                SELECT fw.id
                FROM content.film_work fw
                JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                WHERE gfw.genre_id = ANY(%s)
            """
        else:
            raise ValueError('relation_field должен быть "person_id" или "genre_id"')

        with self.pg_conn.cursor() as cur:
            cur.execute(query, (source_ids,))
            film_rows = cur.fetchall()
            film_ids = {str(row[0]) for row in film_rows}

        # Пишем связанные film_work.id в целевой Redis set/queue
        for fw_id in film_ids:
            redis_conn.sadd(target_queue_name, fw_id)

        print(f'Enricher: нашёл {len(film_ids)} связанных фильмов по {self.source_queue}')
        # (по желанию) Можно сразу удалить обработанные ids из исходного set:
        for sid in source_ids:
            redis_conn.srem(self.source_queue, sid)

        # --- состояние можно обновлять здесь ---
        self.state.set_state('last_enriched_ids', source_ids)
        return film_ids