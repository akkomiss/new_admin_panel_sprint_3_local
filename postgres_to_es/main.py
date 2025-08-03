import logging
import os
import time
import json
from psycopg import OperationalError
from dotenv import load_dotenv

from utils import pg_conn_context, redis_conn_context, connect_es
from state import State, RedisStorage
from producer import PostgresProducer
from enricher import PostgresEnricher
from merger import PostgresMerger
from transformer import Transformer
from loader import ElasticsearchLoader


def process_source(config, producers, enricher, merger, transformer, r_conn):
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
                r_conn.rpush(output_queue_name, json.dumps(doc, default=str))
        else:
            logging.warning("Данные не были трансформированы, т.к. transformer вернул пустой результат.")
    else:
        logging.warning(f"После обогащения для '{source_type}' не осталось фильмов для обработки.")

    last_modified, last_id = source_rows[-1][1], str(source_rows[-1][0])
    producer.state.set_state('last_updated_at', str(last_modified))
    producer.state.set_state('last_id', last_id)
    logging.info(f"Состояние для '{source_type}' обновлено: modified={last_modified}, id={last_id}\n")

def load_data_to_es(es_loader, r_conn, queue_name='processed_movies_queue', batch_size=100):
    """
    Извлекает данные из очереди Redis и загружает их в Elasticsearch пачками.
    """
    logging.info(f"Проверка очереди '{queue_name}' на наличие данных для загрузки в Elasticsearch...")
    
    while r_conn.llen(queue_name) > 0:
        # Извлекаем пачку данных из Redis
        # lrange(key, 0, N-1) - взять N элементов, ltrim(key, N, -1) - обрезать список, оставив все, что после N
        records_to_load_str = r_conn.lrange(queue_name, 0, batch_size - 1)
        records_to_load = [json.loads(rec) for rec in records_to_load_str]
        
        logging.info(f"Извлечено {len(records_to_load)} документов из Redis для загрузки.")
        
        # Загружаем пачку в Elasticsearch
        es_loader.load_to_es(records_to_load)
        
        # Удаляем успешно загруженные данные из очереди
        r_conn.ltrim(queue_name, len(records_to_load), -1)
        logging.info(f"Успешно обработанная пачка удалена из очереди '{queue_name}'.")

    logging.info("Очередь пуста. Загрузка в Elasticsearch завершена на данный момент.")

def main():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    dsl = {
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('SQL_HOST'),
        'port': os.getenv('SQL_PORT')
    }
    
    redis_opts = {'host': os.getenv('REDIS_HOST'), 'port': os.getenv('REDIS_PORT'), 'db': 0, 'decode_responses': True}
    es_host = os.getenv('ELASTIC_HOST', 'localhost')
    es_port = int(os.getenv('ELASTIC_PORT', 9200))
    es_index = 'movies'

    producer_configs = [
        {'source_type': 'film_work', 'table': 'content.film_work', 'state_key': 'film_work_producer', 'enrich': False},
        {'source_type': 'person', 'table': 'content.person', 'state_key': 'person_producer', 'enrich': True},
        {'source_type': 'genre', 'table': 'content.genre', 'state_key': 'genre_producer', 'enrich': True},
    ]

    try:
        with redis_conn_context(**redis_opts) as r_conn, \
             connect_es(hosts=[f"http://{es_host}:{es_port}"]) as es_conn:

            logging.info("Соединения с Redis и Elasticsearch установлены.")
            
            transformer = Transformer()
            loader = ElasticsearchLoader(es_conn, es_index)

            while True:
                try:
                    with pg_conn_context(**dsl) as p_conn:
                        states = {config['state_key']: State(RedisStorage(r_conn, config['state_key'])) for config in producer_configs}
                        producers = {config['source_type']: PostgresProducer(p_conn, states[config['state_key']], config['table']) for config in producer_configs}
                        enricher = PostgresEnricher(p_conn)
                        merger = PostgresMerger(p_conn)
                        
                        for config in producer_configs:
                            process_source(config, producers, enricher, merger, transformer, r_conn)
                
                except OperationalError as e:
                    logging.warning(f"Не удалось подключиться к PostgreSQL в этом цикле. Повтор через 5 секунд. Ошибка: {e}")
                
                # После обработки всех источников, запускаем загрузку в ES
                load_data_to_es(loader, r_conn)

                logging.info("--- Все источники обработаны. Пауза 5 секунд. ---\n")
                time.sleep(1)

    except Exception as e:
        logging.error(f"Критическая ошибка в главном цикле ETL: {e}", exc_info=True)


if __name__ == '__main__':
    main()
