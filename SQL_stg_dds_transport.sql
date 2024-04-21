drop table if exists dds.srv_wf_settings;
create table dds.srv_wf_settings (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE)
	,workflow_key varchar not null
	,workflow_settings json not null
	,CONSTRAINT dds_srv_wf_settings_pkey PRIMARY KEY (id)
	)
	
	
SELECT  id
	,object_value::JSON->>'_id' user_id
	,object_value::JSON->>'name' user_name
	,object_value::JSON->>'login' user_login
FROM stg.ordersystem_users ou



SELECT id 
	,object_value::JSON->>'_id' restaurant_id
	,object_value::JSON->>'name' restaurant_name
	,object_value::JSON->>'update_ts' active_from
	,'2099-12-31 00:00:00.000' active_to
FROM stg.ordersystem_restaurants ors 




SELECT ts ts
	,extract('Year' from ts) as "year"
	,extract('Month' from ts) as "month"
	,extract('Day' from ts) as "day"
	,date(ts) as "date"
	,ts::time as "time"
FROM (	SELECT to_timestamp(object_value::JSON->>'date', 'YYYY-MM-DD HH24:MI:SS') ts
		FROM stg.ordersystem_orders ord ) a
		

		
		
select restaurant_id
	,product_id
	,product_name
	,product_price
	,MIN(order_date) active_from
	,'2099-12-31 00:00:00.000' active_to
	,MAX(update_ts) update_ts
from (
		select 
			json_array_elements(items::json)::JSON->>'id'::varchar as product_id
			,json_array_elements(items::json)::JSON->>'name'::text as product_name
			,(json_array_elements(items::json)::JSON->>'price')::numeric(14,2) as product_price
			,order_date
			,restaurant_id
			,update_ts
		from (
				select dr.id  restaurant_id
					,json(object_value::JSON->>'order_items') items
					,(object_value::JSON->>'date') order_date
					,oo.update_ts update_ts
				from stg.ordersystem_orders oo 
				join dds.dm_restaurants dr 
					on (oo.object_value::JSON->>'restaurant')::JSON->>'id' = dr.restaurant_id
						and oo.update_ts >= dr.active_from
						and oo.update_ts < dr.active_to
				) a
		) b
group by restaurant_id
	,product_id
	,product_name
	,product_price


	
	
	
select ord.order_uid order_key
	,status order_status
	,dr.id restaurant_id
	,dt.id timestamp_id
	,dus.id user_id
	,ord.update_ts update_ts
from (
		select object_value::JSON->>'_id'::varchar order_uid
			,to_timestamp(object_value::JSON->>'date', 'YYYY-MM-DD HH24:MI:SS') ts
			,object_value::JSON->>'final_status'::text status
			,(object_value::JSON->>'restaurant')::JSON->>'id'::varchar restaurant_uid
			,(object_value::JSON->>'user')::JSON->>'id'::varchar user_uid
			,update_ts
		from stg.ordersystem_orders
--		WHERE update_ts > %(threshold)s
		) ord 
join dds.dm_restaurants dr 
	on ord.restaurant_uid = dr.restaurant_id 
join dds.dm_timestamps dt 
	on ord.ts = dt.ts 
join dds.dm_users dus 
	on ord.user_uid = dus.user_id

	
	
	
with 
ord as (
select json_array_elements(order_items::json)::JSON->>'id'::varchar product_uid
	,order_uid
	,order_dt
	,json_array_elements(order_items::json)::JSON->>'quantity'::varchar "count"
	,json_array_elements(order_items::json)::JSON->>'price'::varchar price
	,update_ts
from (	
		select object_value::JSON->>'date'::varchar order_dt
			,object_value::JSON->>'order_items'::varchar order_items
			,object_value::JSON->>'_id'::varchar order_uid
			,update_ts		
		from stg.ordersystem_orders
--		WHERE update_ts > %(threshold)s
		) od
	),
bns as (
select order_uid
	,json_array_elements(product_payments::json)::JSON->>'product_id'::varchar product_uid
	,(json_array_elements(product_payments::json)::JSON->>'price')::numeric(19, 5) price
	,(json_array_elements(product_payments::json)::JSON->>'quantity')::int quantity
	,((json_array_elements(product_payments::json)::JSON->>'price')::numeric(19,5)
		* (json_array_elements(product_payments::json)::JSON->>'quantity')::numeric(19,5)) total_sum
	,(json_array_elements(product_payments::json)::JSON->>'bonus_payment')::numeric(19, 5) bonus_payment
	,(json_array_elements(product_payments::json)::JSON->>'bonus_grant')::numeric(19, 5) bonus_grant
	,order_date
from (	
		select 
			event_value::JSON->>'order_id'::varchar order_uid
			,event_value::JSON->>'product_payments'::varchar product_payments
			,event_value::JSON->>'order_date'::varchar order_date
		from stg.bonussystem_events be 
		where event_type = 'bonus_transaction'
		) bs
	)
select dp.id product_id 
	,dor.id order_id 
	,bns.quantity "count"
	,bns.price price
	,bns.total_sum total_sum
	,bns.bonus_payment bonus_payment
	,bns.bonus_grant bonus_grant
	,update_ts
from bns as bns 
join ord as ord
	on bns.order_uid = ord.order_uid
	and bns.product_uid = ord.product_uid
	and bns.order_date = ord.order_dt
join dds.dm_products dp 
	on ord.product_uid = dp.product_id
join dds.dm_orders dor 
	on ord.order_uid = dor.order_key 
		
	
	
	
	
                SELECT
                    o.order_key AS order_key,
                    dp.product_id AS product_id,
                    pr.total_sum,
                    pr.count,
                    pr.price,
                    pr.bonus_payment,
                    pr.bonus_grant
                FROM dds.fct_product_sales AS pr
                    INNER JOIN dds.dm_orders AS o
                        ON pr.order_id = o.id
                    INNER JOIN dds.dm_timestamps AS t
                        ON o.timestamp_id = t.id
                    INNER JOIN dds.dm_products AS dp 
                        ON pr.product_id = dp.id
                WHERE ts::date BETWEEN (now() AT TIME ZONE 'utc')::date - 2 AND (now() AT TIME ZONE 'utc')::date - 1                        
                ORDER BY ts desc
                
                
                
insert into cdm.dm_settlement_report (
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
where dor.order_status = 'CLOSED'
group by dre.id
	,dre.restaurant_name
	,dt."date"

