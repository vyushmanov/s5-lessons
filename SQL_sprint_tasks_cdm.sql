--  --
drop table if exists cdm.dm_settlement_report;
CREATE TABLE cdm.dm_settlement_report
    (id serial
    ,restaurant_id  varchar  NOT Null
    ,restaurant_name varchar  NOT Null
    ,settlement_date date NOT Null
    ,orders_count integer NOT Null
    ,orders_total_sum numeric(14, 2) NOT Null
    ,orders_bonus_payment_sum numeric(14, 2) NOT Null
    ,orders_bonus_granted_sum numeric(14, 2) NOT Null
    ,order_processing_fee numeric(14, 2) NOT Null
    ,restaurant_reward_sum numeric(14, 2) NOT Null
    );

ALTER TABLE cdm.dm_settlement_report
ADD PRIMARY KEY (id);
  
ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT dm_settlement_report_settlement_date_check 
	CHECK (settlement_date >= '2022-01-01' AND settlement_date < '2500-01-01');

ALTER TABLE cdm.dm_settlement_report ALTER COLUMN orders_count SET DEFAULT 0;
ALTER TABLE cdm.dm_settlement_report ALTER COLUMN orders_total_sum SET DEFAULT 0;
ALTER TABLE cdm.dm_settlement_report ALTER COLUMN orders_bonus_payment_sum SET DEFAULT 0;
ALTER TABLE cdm.dm_settlement_report ALTER COLUMN orders_bonus_granted_sum SET DEFAULT 0;
ALTER TABLE cdm.dm_settlement_report ALTER COLUMN order_processing_fee SET DEFAULT 0;
ALTER TABLE cdm.dm_settlement_report ALTER COLUMN restaurant_reward_sum SET DEFAULT 0;

ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_count_check CHECK (orders_count>=0);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_total_sum_check CHECK (orders_total_sum >= (0)::numeric);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check CHECK (orders_bonus_payment_sum >= (0)::numeric);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check CHECK (orders_bonus_granted_sum >= (0)::numeric);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_order_processing_fee_check CHECK (order_processing_fee >= (0)::numeric);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_restaurant_reward_sum_check CHECK (restaurant_reward_sum >= (0)::numeric);

ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_unique_constraint UNIQUE (restaurant_id, settlement_date);
