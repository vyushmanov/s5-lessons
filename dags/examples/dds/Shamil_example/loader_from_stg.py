from logging import Logger
from typing import List

from examples.dds import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class ProductObj(BaseModel):
    restaurant_id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: str
    active_to: str
    threshold_key: str

class ProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, threshold: str, limit: int) -> List[ProductObj]:
        with self._db.client().cursor(row_factory=class_row(ProductObj)) as cur:
            cur.execute(
                f"""
                -- +++
                with restaurant_products as (
                    -- restaurant products
                    select 
                    restaurant_guid,
                    (menu::JSON->>'_id')::varchar as product_id,
                    (menu::JSON->>'category')::text as product_category,
                    (menu::JSON->>'name')::text as product_name,
                    (menu::JSON->>'price')::numeric (14,2) as product_price,
                        restautant_active_from,
                        restautant_active_to
                    from
                    (
                        select 
                            restaurant_guid,
                            json_array_elements(menu::json) as menu,
                            active_from as restautant_active_from,
                            active_to as restautant_active_to
                        from
                        (
                        select 
                            (object_value::JSON->>'_id') as restaurant_guid,
                            object_value::JSON->>'menu' as menu,
                            -- update_ts as restautant_update_ts
                            update_ts as active_from,
                            '2099-12-31T00:00:00.000Z'::timestamp  as active_to		     
                        from stg.ordersystem_restaurants
                        ) o
                    ) i
                )
                ,
                restaurant_ids_and_product_ids as (
                select distinct
                    o.id as restaurant_id, 
                    p.restaurant_guid, 
                    p.product_id,
                    p.restautant_active_from as active_from,
                    p.restautant_active_to as active_to
                from restaurant_products p
                inner join stg.ordersystem_restaurants o on p.restaurant_guid = o.object_id
                )

                , stg_to_dm_order_products_import as (

                -- orders products
                select distinct
                    r.restaurant_id,
                    p.product_id,
                    p.product_name,
                    p.product_price,
                    p.product_quantity,
                    -- p.active_from,
                    -- p.active_to
                    r.active_from,
                    r.active_to    
                from 
                (
                select
                ts,
                (order_items::JSON->>'id')::varchar as product_id,
                (order_items::JSON->>'name')::text as product_name,
                (order_items::JSON->>'price')::numeric (14,2) as product_price,
                (order_items::JSON->>'quantity')::int as product_quantity,
                update_ts as active_from,
                '2099-12-31T00:00:00.000Z'::timestamp  as active_to
                from
                (
                    select 
                        ts,
                        json_array_elements(order_items::json) as order_items,
                        update_ts
                    from
                    (
                    select 
                        (object_value::JSON->>'date')::timestamp as ts,
                        object_value::JSON->>'order_items' as order_items,
                        (object_value::JSON->>'final_status') as order_status,   
                        update_ts
                    from stg.ordersystem_orders
                        where (object_value::JSON->>'final_status') in ('CLOSED', 'CANCELLED')
                    ) o
                ) i
                ) p
                inner join restaurant_ids_and_product_ids r on p.product_id = r.product_id
                order by r.active_from, p.product_id, r.restaurant_id -- product_name
                )
                ,
                import_records as (
                select 
                    restaurant_id,
                    product_id,
                    product_name,
                    product_price,
                    product_quantity,
                    -- active_from,
                    to_char (active_from::timestamp at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as active_from,
                    -- p.active_to
                    to_char (active_to::timestamp at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as active_to,
                    (
                        product_Id ||
                        TO_CHAR(active_from, 'YYYY-MM-DD T HH24:MI:SS' ||
                        to_char(restaurant_id, '0000009')                     
                    )) threshold_key      
                from 
                    stg_to_dm_order_products_import
                )
                select 
                    distinct 
                        product_id, 
                        product_name,
                        product_price, 
                        active_from, 
                        active_to, 
                        restaurant_id,
                        threshold_key 
                from import_records 
                -- +++                
                WHERE threshold_key > '{threshold}' --Пропускаем те объекты, которые уже загрузили.
                ORDER BY threshold_key ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                LIMIT {limit}; --Обрабатываем только одну пачку объектов.
                """                
            )
            objs = cur.fetchall()
        return objs


class ProductsDestRepository:

    def insert_product(self, conn: Connection, product: ProductObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (restaurant_id, product_id, active_from) DO UPDATE
                    SET
                        product_name = EXCLUDED.product_name,
                        product_price = EXCLUDED.product_price,
                        active_to = EXCLUDED.active_to
                    ;
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


class ProductsLoader:
    WF_KEY = "products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "id"
    BATCH_LIMIT = 50  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProductsOriginRepository(pg_origin)
        self.stg = ProductsDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
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
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for product in load_queue:
                self.stg.insert_product(conn, product)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            # wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.threshold_key for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
