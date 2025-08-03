import logging
import time
import json
from psycopg import OperationalError
from redis import Redis

from utils import pg_conn_context, redis_conn_context, connect_es
from state import State, RedisStorage
from producer import PostgresProducer
from enricher import PostgresEnricher
from merger import PostgresMerger
from transformer import PostgresTransformer
from loader import ElasticsearchLoader
from config import settings

def process_source(config: dict, producers: dict[str, PostgresProducer], enricher: PostgresEnricher, merger: PostgresMerger, transformer: PostgresTransformer, redis_connection: Redis):
    """
    Выполняет полный цикл ETL для одного источника данных (одной таблицы).
    """
    source_type = config['source_type']
    producer = producers[source_type]

    logging.info(f"-> Запуск producer для '{source_type}'...")
    source_rows = producer.extract()
    if not source_rows:
        logging.info(f"Для '{source_type}' нет новых данных.")
        return

    logging.info(f"Producer извлек {len(source_rows)} записей из '{source_type}'.")

    source_ids = [row[0] for row in source_rows]
    film_work_ids = enricher.enrich(source_ids, source_type) if config['enrich'] else source_ids

    if film_work_ids:
        logging.info(f"Подготовка к обработке данных для {len(film_work_ids)} фильмов.")

        raw_data = merger.fetch_merged_data(film_work_ids)
        transformed_data = transformer.transform_data(raw_data)

        if transformed_data:
            output_queue_name = 'processed_movies_queue'
            logging.info(f"Отправка {len(transformed_data)} документов в очередь '{output_queue_name}'...")
            for doc in transformed_data:
                redis_connection.rpush(output_queue_name, json.dumps(doc, default=str))
        else:
            logging.warning("Данные не были трансформированы, т.к. transformer вернул пустой результат.")
    else:
        logging.warning(f"После обогащения для '{source_type}' не осталось фильмов для обработки.")

    last_modified, last_id = source_rows[-1][1], str(source_rows[-1][0])
    producer.state.set_state('last_updated_at', str(last_modified))
    producer.state.set_state('last_id', last_id)
    logging.info(f"Состояние для '{source_type}' обновлено: modified={last_modified}, id={last_id}\n")

def load_data_to_es(es_loader: ElasticsearchLoader, redis_connection: Redis, queue_name: str = 'processed_movies_queue'):
    """
    Извлекает данные из очереди Redis и загружает их в Elasticsearch пачками.
    """
    logging.info(f"Проверка очереди '{queue_name}' на наличие данных для загрузки в Elasticsearch...")

    while redis_connection.llen(queue_name) > 0:
        records_to_load_str = redis_connection.lrange(queue_name, 0, settings.batch_size - 1)
        records_to_load = [json.loads(rec) for rec in records_to_load_str]

        logging.info(f"Извлечено {len(records_to_load)} документов из Redis для загрузки.")

        es_loader.load_to_es(records_to_load)

        redis_connection.ltrim(queue_name, len(records_to_load), -1)
        logging.info(f"Успешно обработанная пачка удалена из очереди '{queue_name}'.")

    logging.info("Очередь пуста. Загрузка в Elasticsearch завершена на данный момент.")

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        with redis_conn_context(**settings.redis.to_dict()) as redis_connection, \
             connect_es(hosts=[f"http://{settings.es.host}:{settings.es.port}"]) as es_conn:

            logging.info("Соединения с Redis и Elasticsearch установлены.")

            loader = ElasticsearchLoader(es_conn, settings.es.index)

            while True:
                try:
                    with pg_conn_context(**settings.pg.to_dict()) as p_conn:
                        states = {config.state_key: State(RedisStorage(redis_connection, config.state_key)) for config in settings.producer_configs}
                        producers = {config.source_type: PostgresProducer(p_conn, states[config.state_key], config.table, settings.batch_size) for config in settings.producer_configs}
                        enricher = PostgresEnricher(p_conn, settings.batch_size)
                        merger = PostgresMerger(p_conn, settings.batch_size)
                        transformer = PostgresTransformer(enricher)

                        for config in settings.producer_configs:
                            process_source(config.model_dump(), producers, enricher, merger, transformer, redis_connection)

                except OperationalError as e:
                    logging.warning(f"Не удалось подключиться к PostgreSQL в этом цикле. Повтор через 5 секунд. Ошибка: {e}")

                load_data_to_es(loader, redis_connection)

                logging.info(f"--- Все источники обработаны. Пауза {settings.sleep_time} секунд. ---\n")
                time.sleep(settings.sleep_time)

    except Exception as e:
        logging.error(f"Критическая ошибка в главном цикле ETL: {e}", exc_info=True)


if __name__ == '__main__':
    main()
