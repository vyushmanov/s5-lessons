from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class ProductObj(BaseModel):
    # id: int
    restaurant_id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime

class ProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, product_threshold: int, limit: int) -> List[ProductObj]:
        with self._db.client().cursor(row_factory=class_row(ProductObj)) as cur:
            cur.execute(
                """
                    SELECT restaurant_id
                        ,product_id
                        ,product_name
                        ,product_price
                        ,MIN(order_date) active_from
                        ,'2099-12-31 00:00:00.000' active_to
                        ,MAX(update_ts) update_ts
                    FROM (
                            SELECT 
                                json_array_elements(items::json)::JSON->>'id'::varchar as product_id
                                ,json_array_elements(items::json)::JSON->>'name'::text as product_name
                                ,(json_array_elements(items::json)::JSON->>'price')::numeric(14,2) as product_price
                                ,order_date
                                ,restaurant_id
                                ,update_ts
                            FROM (
                                    SELECT dr.id  restaurant_id
                                        ,json(object_value::JSON->>'order_items') items
                                        ,(object_value::JSON->>'date') order_date
                                        ,oo.update_ts update_ts
                                    FROM stg.ordersystem_orders oo 
                                    JOIN dds.dm_restaurants dr 
                                        ON (oo.object_value::JSON->>'restaurant')::JSON->>'id' = dr.restaurant_id
                                            AND oo.update_ts >= dr.active_from
                                            AND oo.update_ts < dr.active_to
                                    WHERE oo.update_ts > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                                    ) a
                            ) b
                    GROUP BY restaurant_id
                        ,product_id
                        ,product_name
                        ,product_price
                    ORDER BY update_ts ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    -- LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": product_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class ProductsDestRepository:

    def insert_product(self, conn: Connection, product: ProductObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products (restaurant_id, product_id, product_name, product_price, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (restaurant_id, product_id, active_to) DO NOTHING
                """,
                {
                    "restaurant_id": product.restaurant_id,
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_price": product.product_price,
                    "active_from": product.active_from,
                    "active_to": product.active_to
                },
            )

    def update_product(self, conn: Connection, product: ProductObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    UPDATE dds.dm_products
                    SET active_to = %(active_from)s
                    WHERE restaurant_id = %(restaurant_id)s
                        AND product_id = %(product_id)s
                        AND product_price != %(product_price)s
                        AND active_to = '2099-12-31 00:00:00.000'
                """,
                {
                    "restaurant_id": product.restaurant_id,
                    "product_id": product.product_id,
                    "product_price": product.product_price,
                    "active_from": product.active_from,
                },
            )

class ProductLoader:
    WF_KEY = "example_products_from_orders_stg_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100  #

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProductsOriginRepository(pg_origin)
        self.dds = ProductsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_products(self):
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
            load_queue = self.origin.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            print('Строка из таблицы: ', load_queue[0])
            # Сохраняем объекты в базу dwh.
            for product in load_queue:
                self.dds.insert_product(conn, product)
                self.dds.update_product(conn, product)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t.active_from for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
