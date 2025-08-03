from psycopg import OperationalError
from utils import backoff

class PostgresProducer:
    def __init__(self, pg_conn, state, table, batch_size=100):
        self.pg_conn = pg_conn
        self.state = state
        self.table = table
        self.batch_size = batch_size

    @backoff(exceptions=(OperationalError,), service_name="PostgreSQL")
    def extract(self):
        last_updated = self.state.get_state('last_updated_at', '1970-01-01T00:00:00+00:00')
        last_id = self.state.get_state('last_id', '00000000-0000-0000-0000-000000000000')
        
        query = f"""
            SELECT id, updated_at
            FROM {self.table}
            WHERE (updated_at, id) > (%s, %s)
            ORDER BY updated_at, id
            LIMIT %s;
        """
        with self.pg_conn.cursor() as cur:
            cur.execute(query, (last_updated, last_id, self.batch_size))
            rows = cur.fetchall()
        return rows
