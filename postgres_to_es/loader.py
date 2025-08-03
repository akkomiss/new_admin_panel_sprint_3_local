import logging
from elasticsearch import Elasticsearch, helpers, ConnectionError
from utils import backoff

class ElasticsearchLoader:
    def __init__(self, es_conn: Elasticsearch, index_name: str):
        self.es_conn = es_conn
        self.index_name = index_name

    @backoff(exceptions=(ConnectionError,), service_name="Elasticsearch")
    def load_to_es(self, records: list[dict]):
        """
        Загружает пачку документов в Elasticsearch.
        В случае сбоя соединения будет повторять попытки благодаря декоратору @backoff.
        """
        if not records:
            return

        actions = [
            {
                "_index": self.index_name,
                "_id": record['id'],
                "_source": record
            }
            for record in records
        ]

        try:
            success, failed = helpers.bulk(self.es_conn, actions)
            logging.info(f"Успешно загружено: {success}. Не удалось загрузить: {failed}.")
            if failed:
                # Эти ошибки не связаны со сбоем соединения, а с самими данными.
                # Backoff их не поймает, они будут просто залогированы.
                logging.error(f"Ошибки при загрузке данных (не связаны с соединением): {failed}")

        except Exception as e:
            # Эта секция может поймать другие ошибки, не ConnectionError
            logging.error(f"Непредвиденная ошибка при bulk-загрузке в Elasticsearch: {e}")
            raise
