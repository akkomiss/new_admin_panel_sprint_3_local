"""Модуль для управления конфигурацией ETL процесса."""
import logging
from typing import List, Dict, Any
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseSettings):
    """Настройки для подключения к PostgreSQL."""
    dbname: str = Field(..., validation_alias='POSTGRES_DB')
    user: str = Field(..., validation_alias='POSTGRES_USER')
    password: str = Field(..., validation_alias='POSTGRES_PASSWORD')
    host: str = Field(..., validation_alias='SQL_HOST')
    port: int = Field(..., validation_alias='SQL_PORT')

    def to_dict(self) -> Dict[str, Any]:
        """Возвращает настройки в виде словаря для psycopg."""
        return self.model_dump()


class RedisSettings(BaseSettings):
    """Настройки для подключения к Redis."""
    host: str = Field(..., validation_alias='REDIS_HOST')
    port: int = Field(..., validation_alias='REDIS_PORT')
    db: int = 0
    decode_responses: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Возвращает настройки в виде словаря для redis-py."""
        return self.model_dump()


class ElasticsearchSettings(BaseSettings):
    """Настройки для подключения к Elasticsearch."""
    host: str = Field(..., validation_alias='ELASTIC_HOST')
    port: int = Field(..., validation_alias='ELASTIC_PORT')
    index: str = 'movies'


class ProducerConfig(BaseModel):
    """Конфигурация для одного источника данных (продюсера)."""
    source_type: str
    table: str
    state_key: str
    enrich: bool


class AppSettings(BaseSettings):
    """Основной класс с настройками приложения."""
    model_config = SettingsConfigDict(
        env_nested_delimiter='__',
        env_file_encoding='utf-8'
    )

    pg: PostgresSettings = Field(default_factory=PostgresSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    es: ElasticsearchSettings = Field(default_factory=ElasticsearchSettings)

    producer_configs: List[ProducerConfig] = [
        ProducerConfig(source_type='film_work', table='content.film_work', state_key='film_work_producer', enrich=False),
        ProducerConfig(source_type='person', table='content.person', state_key='person_producer', enrich=True),
        ProducerConfig(source_type='genre', table='content.genre', state_key='genre_producer', enrich=True),
    ]
    batch_size: int = Field(100, validation_alias='BATCH_SIZE')
    sleep_time: int = Field(1, validation_alias='SLEEP_TIME')


try:
    settings = AppSettings()
except Exception as e:
    logging.error(f"Ошибка при загрузке конфигурации: {e}")
    # Выход или использование настроек по-умолчанию, если это возможно
    raise
