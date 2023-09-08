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