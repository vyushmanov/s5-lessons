import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.Shamil_example.loader_from_stg import ProductsLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_stg_to_dds_products_loader_dag():
    # Создаем подключение к базе dwh.
    dds_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    # Создаем подключение к базе подсистемы бонусов.
    stg_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="products_load")
    def load_products():
        # создаем экземпляр класса, в котором реализована логика.
        products_loader = ProductsLoader(stg_pg_connect, dds_pg_connect, log)
        products_loader.load_products()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    products_dict = load_products()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    products_dict  # type: ignore


stg_to_dds_products_loader_dag = sprint5_stg_to_dds_products_loader_dag()
