from state import State, RedisStorage

class PostgresMerger:
    """
    Берёт id фильмов из Redis, собирает полный набор данных для Elasticsearch.
    """
    def __init__(self, pg_conn, state: State, queue_key: str, batch_size=100):
        self.pg_conn = pg_conn
        self.state = state
        self.queue_key = queue_key  # Например, 'film_work_ids'
        self.batch_size = batch_size

    def fetch_and_merge(self, redis_conn):
        # Забираем пачку id фильмов из Redis (set)
        ids = redis_conn.srandmember(self.queue_key, self.batch_size)
        ids = [i.decode() if isinstance(i, bytes) else str(i) for i in ids]
        if not ids:
            return []

        # Многострочный "широкий" запрос по этим id
        query = """
            SELECT
                fw.id as fw_id, 
                fw.title, 
                fw.description, 
                fw.rating, 
                fw.type, 
                fw.created_at, 
                fw.updated_at, 
                pfw.role, 
                p.id as person_id, 
                p.full_name,
                g.name as genre_name
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id = ANY(%s)
        """
        with self.pg_conn.cursor() as cur:
            cur.execute(query, (ids,))
            rows = cur.fetchall()

        # Можно добавить обработку данных: сгруппировать по fw.id, собрать списки участников, жанров и т.п.
        # Это твой "Transform"-этап перед отправкой в Elasticsearch

        # После успешной обработки — удаляем обработанные id из очереди
        for i in ids:
            redis_conn.srem(self.queue_key, i)

        # Сохраняем состояние (по желанию — например, последний fw_id)
        self.state.set_state('last_merged_ids', ids)
        print(f"Merger: обработано {len(rows)} строк по {len(ids)} фильмам")
        return rows