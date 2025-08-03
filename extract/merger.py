from psycopg import OperationalError
from utils import backoff

class PostgresMerger:
    def __init__(self, pg_conn):
        self.pg_conn = pg_conn
    
    @backoff(exceptions=(OperationalError,), service_name="PostgreSQL")
    def merge(self, film_work_ids):
        if not film_work_ids:
            return []

        placeholders = ','.join(['%s'] * len(film_work_ids))
        query = f"""
            SELECT
                fw.id,
                fw.title,
                fw.description,
                fw.rating AS imdb_rating,
                fw.created_at,
                fw.updated_at,
                COALESCE(
                    json_agg(DISTINCT jsonb_build_object('id', g.id, 'name', g.name))
                    FILTER (WHERE g.id IS NOT NULL),
                    '[]'
                ) AS genres,
                COALESCE(
                    json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
                    FILTER (WHERE p.id IS NOT NULL AND pfw.role = 'actor'),
                    '[]'
                ) AS actors,
                COALESCE(
                    json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
                    FILTER (WHERE p.id IS NOT NULL AND pfw.role = 'writer'),
                    '[]'
                ) AS writers,
                COALESCE(
                    json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
                    FILTER (WHERE p.id IS NOT NULL AND pfw.role = 'director'),
                    '[]'
                ) AS directors
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN ({placeholders})
            GROUP BY fw.id;
        """
        with self.pg_conn.cursor() as cur:
            cur.execute(query, [str(fw_id) for fw_id in film_work_ids])
            colnames = [desc[0] for desc in cur.description]
            merged_rows = [dict(zip(colnames, row)) for row in cur.fetchall()]
            
        return merged_rows
