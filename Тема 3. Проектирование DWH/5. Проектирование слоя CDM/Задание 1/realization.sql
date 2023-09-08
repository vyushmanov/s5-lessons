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
	,PRIMARY key (id)
    );