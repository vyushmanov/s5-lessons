with
res as (select ra.id id
		from public_test.dm_settlement_report_expected re
		full join public_test.dm_settlement_report_actual ra
			on re.restaurant_id = ra.restaurant_id
			and re.restaurant_name = ra.restaurant_name
			and re.settlement_year = ra.settlement_year
			and re.settlement_month = ra.settlement_month
			and re.orders_count = ra.orders_count
			and re.orders_total_sum = ra.orders_total_sum
			and re.orders_bonus_granted_sum = ra.orders_bonus_granted_sum
			and re.order_processing_fee = ra.order_processing_fee
			and re.restaurant_reward_sum = ra.restaurant_reward_sum
		where re.id is null )
--insert into public_test.testing_result
--	(test_date_time, test_name, test_result)
select now() test_date_time
	,'test_01' test_name
	,(case when count(id)=0 then 1 else 0 end)::boolean test_result
from res