from psycopg import OperationalError
from utils import backoff

class PostgresEnricher:
    # Оставляем только те поля, что реально нужны для обогащения
    RELATION_MAPPING = {
        'person': ('content.person_film_work', 'person_id'),
        'genre': ('content.genre_film_work', 'genre_id'),
    }

    def __init__(self, pg_conn, chunk_size):
        self.pg_conn = pg_conn
        self.chunk_size = chunk_size
    
    @backoff(exceptions=(OperationalError,), service_name="PostgreSQL")
    def enrich(self, source_ids, source_type):
        if not source_ids or source_type not in self.RELATION_MAPPING:
            return []
            
        m2m_table, relation_field = self.RELATION_MAPPING[source_type]

        placeholders = ','.join(['%s'] * len(source_ids))
        query = f"""
            SELECT DISTINCT fw.id
            FROM content.film_work fw
            LEFT JOIN {m2m_table} pfw ON pfw.film_work_id = fw.id
            WHERE pfw.{relation_field} IN ({placeholders});
        """
        with self.pg_conn.cursor() as cur:
            cur.execute(query, [str(sid) for sid in source_ids])
            film_work_ids = []
            while True:
                rows = cur.fetchmany(self.chunk_size)
                if not rows:
                    break
                film_work_ids.extend([row[0] for row in rows])
        return film_work_ids
