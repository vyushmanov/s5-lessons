import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.bonus_system_outbox_dag.outbox_loader import OutboxLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_stg_bonus_system_outbox_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="outbox_load")
    def load_outbox():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = OutboxLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_outbox()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    outbox_dict = load_outbox()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    outbox_dict  # type: ignore


stg_bonus_system_outbox_dag = sprint5_example_stg_bonus_system_outbox_dag()
