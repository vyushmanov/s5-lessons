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

    def settlement_report_isert(self, fct_product_sales_threshold: int, limit: int) -> List[FctProductSalesObj]:
        with self._db.client().cursor(row_factory=class_row(FctProductSalesObj)) as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_settlement_report (
                            restaurant_id,restaurant_name,settlement_date,orders_count,orders_total_sum
                            ,orders_bonus_payment_sum,orders_bonus_granted_sum,order_processing_fee,restaurant_reward_sum)
                    select 
                        dre.restaurant_id restaurant_id
                        ,dre.restaurant_name  restaurant_name
                        ,dt."date" settlement_date
                        ,COUNT(distinct fps.order_id)  orders_count
                        ,SUM(fps.total_sum) orders_total_sum
                        ,SUM(fps.bonus_payment) orders_bonus_payment_sum
                        ,SUM(fps.bonus_grant)  orders_bonus_granted_sum
                        ,SUM(0.25 * fps.total_sum) order_processing_fee
                        ,SUM(0.75 * fps.total_sum - fps.bonus_payment) restaurant_reward_sum
                    from dds.fct_product_sales fps 
                    join dds.dm_orders dor 
                        on fps.order_id = dor.id 
                    join dds.dm_restaurants dre 
                        on dor.restaurant_id = dre.id
                    join dds.dm_timestamps dt 
                        on dor.timestamp_id = dt.id	
                    WHERE  dor.order_status = 'CLOSED'
                    group by dre.id
                        ,dre.restaurant_name
                        ,dt."date"
                                )
                                objs = cur.fetchall()
                            return objs
                    """
            )


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
            self.origin.settlement_report_isert(last_loaded, self.BATCH_LIMIT)
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
