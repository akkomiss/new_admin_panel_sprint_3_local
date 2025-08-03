from psycopg import OperationalError
from utils import backoff

class PostgresMerger:
    def __init__(self, pg_conn, chunk_size):
        self.pg_conn = pg_conn
        self.chunk_size = chunk_size
    
    @backoff(exceptions=(OperationalError,), service_name="PostgreSQL")
    def fetch_merged_data(self, film_work_ids):
        """
        Извлекает 'плоские' данные для указанных film_work_ids.
        """
        if not film_work_ids:
            return []

        placeholders = ','.join(['%s'] * len(film_work_ids))
        
        query = f"""
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
                g.id as genre_id,
                g.name as genre_name
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN ({placeholders});
        """

        with self.pg_conn.cursor() as cur:
            cur.execute(query, [str(fw_id) for fw_id in film_work_ids])
            colnames = [desc[0] for desc in cur.description]
            
            merged_data = []
            while True:
                rows = cur.fetchmany(self.chunk_size)
                if not rows:
                    break
                merged_data.extend([dict(zip(colnames, row)) for row in rows])
            
        return merged_data
