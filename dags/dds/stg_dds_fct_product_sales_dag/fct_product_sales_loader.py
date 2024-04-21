from logging import Logger
from typing import List

from dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class FctProductSalesObj(BaseModel):
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float
    update_ts: datetime

class FctProductSalesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_fct_product_saless(self, fct_product_sales_threshold: int, limit: int) -> List[FctProductSalesObj]:
        with self._db.client().cursor(row_factory=class_row(FctProductSalesObj)) as cur:
            cur.execute(
                """
                    WITH 
                    ord as (
                    SELECT json_array_elements(order_items::json)::JSON->>'id'::varchar product_uid
                        ,order_uid
                        ,json_array_elements(order_items::json)::JSON->>'quantity'::varchar "count"
                        ,json_array_elements(order_items::json)::JSON->>'price'::varchar price
                        ,update_ts
                    FROM (	
                            SELECT object_value::JSON->>'order_items'::varchar order_items
                                ,object_value::JSON->>'_id'::varchar order_uid
                                ,update_ts		
                            FROM stg.ordersystem_orders
                            WHERE update_ts > %(threshold)s
                            ) od
                        ),
                    bns as (
                    SELECT order_uid
                        ,json_array_elements(product_payments::json)::JSON->>'product_id'::varchar product_uid
                        ,(json_array_elements(product_payments::json)::JSON->>'price')::numeric(19, 5) price
                        ,(json_array_elements(product_payments::json)::JSON->>'quantity')::int quantity
                        ,((json_array_elements(product_payments::json)::JSON->>'price')::numeric(19,5)
                            * (json_array_elements(product_payments::json)::JSON->>'quantity')::numeric(19,5)) total_sum
                        ,(json_array_elements(product_payments::json)::JSON->>'bonus_payment')::numeric(19, 5) bonus_payment
                        ,(json_array_elements(product_payments::json)::JSON->>'bonus_grant')::numeric(19, 5) bonus_grant
                    FROM (	
                            SELECT 
                                event_value::JSON->>'order_id'::varchar order_uid
                                ,event_value::JSON->>'product_payments'::varchar product_payments
                            FROM stg.bonussystem_events be 
                            where event_type = 'bonus_transaction'
                            ) bs
                        )
                    SELECT dp.id product_id 
                        ,dor.id order_id 
                        ,bns.quantity "count"
                        ,bns.price price
                        ,bns.total_sum total_sum
                        ,bns.bonus_payment bonus_payment
                        ,bns.bonus_grant bonus_grant
                        ,update_ts
                    FROM bns as bns 
                    JOIN ord as ord
                        ON bns.order_uid = ord.order_uid
                        AND bns.product_uid = ord.product_uid
                    JOIN dds.dm_products dp 
                        ON ord.product_uid = dp.product_id
                    JOIN dds.dm_orders dor 
                        ON ord.order_uid = dor.order_key
                    ORDER BY update_ts ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    -- LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": fct_product_sales_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class FctProductSalesDestRepository:

    def insert_fct_product_sales(self, conn: Connection, fct_product_sales: FctProductSalesObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales (product_id,order_id,count,price,total_sum,bonus_payment,bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                    ON CONFLICT (product_id, order_id) DO UPDATE
                    SET
                        product_id = EXCLUDED.product_id,
                        order_id = EXCLUDED.order_id;
                """,
                {
                    "product_id": fct_product_sales.product_id,
                    "order_id": fct_product_sales.order_id,
                    "count": fct_product_sales.count,
                    "price": fct_product_sales.price,
                    "total_sum": fct_product_sales.total_sum,
                    "bonus_payment": fct_product_sales.bonus_payment,
                    "bonus_grant": fct_product_sales.bonus_grant
                },
            )


class FctProductSalesLoader:
    WF_KEY = "fct_product_sales_stg_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 100  #

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = FctProductSalesOriginRepository(pg_origin)
        self.dds = FctProductSalesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_fct_product_sales(self):
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
            load_queue = self.origin.list_fct_product_saless(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            print('Строка из таблицы: ', load_queue[0])
            # Сохраняем объекты в базу dwh.
            for fct_product_sales in load_queue:
                self.dds.insert_fct_product_sales(conn, fct_product_sales)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t.update_ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
