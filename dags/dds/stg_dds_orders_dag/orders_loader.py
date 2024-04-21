from logging import Logger
from typing import List

from dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class OrderObj(BaseModel):
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int
    update_ts: datetime

class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, order_threshold: int, limit: int) -> List[OrderObj]:
        with self._db.client().cursor(row_factory=class_row(OrderObj)) as cur:
            cur.execute(
                """
                    SELECT ord.order_uid order_key
                        ,status order_status
                        ,dr.id restaurant_id
                        ,dt.id timestamp_id
                        ,dus.id user_id
                        ,ord.update_ts update_ts
                    FROM (
                            SELECT object_value::JSON->>'_id'::varchar order_uid
                                ,to_timestamp(object_value::JSON->>'date', 'YYYY-MM-DD HH24:MI:SS') ts
                                ,object_value::JSON->>'final_status'::text status
                                ,(object_value::JSON->>'restaurant')::JSON->>'id'::varchar restaurant_uid
                                ,(object_value::JSON->>'user')::JSON->>'id'::varchar user_uid
                                ,update_ts
                            FROM stg.ordersystem_orders
                            WHERE update_ts > %(threshold)s
                            ) ord 
                    JOIN dds.dm_restaurants dr 
                        ON ord.restaurant_uid = dr.restaurant_id 
                    JOIN dds.dm_timestamps dt 
                        ON ord.ts = dt.ts 
                    JOIN dds.dm_users dus 
                        ON ord.user_uid = dus.user_id
                    ORDER BY update_ts ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    -- LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": order_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class OrdersDestRepository:

    def insert_order(self, conn: Connection, order: OrderObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders (user_id, restaurant_id, timestamp_id, order_key, order_status)
                    VALUES (%(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(order_key)s, %(order_status)s)
                    ON CONFLICT (order_key) DO UPDATE
                    SET
                    --    user_id = EXCLUDED.user_id,
                    --    restaurant_id = EXCLUDED.restaurant_id,
                    --    timestamp_id = EXCLUDED.timestamp_id,
                        order_key = EXCLUDED.order_key;
                    --    order_status = EXCLUDED.order_status;
                """,
                {
                    "user_id": order.user_id,
                    "restaurant_id": order.restaurant_id,
                    "timestamp_id": order.timestamp_id,
                    "order_key": order.order_key,
                    "order_status": order.order_status
                },
            )


class OrderLoader:
    WF_KEY = "orders_from_stg_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100  #

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrdersOriginRepository(pg_origin)
        self.dds = OrdersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).

        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        # JSON ничего не знает про даты. Поэтому записываем строку, которую будем кастить при использовании.
                        # А в БД мы сохраним именно JSON.
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            print('Строка из таблицы: ', load_queue[0])
            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                self.dds.insert_order(conn, order)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t.update_ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
