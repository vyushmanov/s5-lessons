with
first_t as (
		select event_value::JSON->>'product_payments' as product_payments
		from public.outbox o
		where event_value::JSON->>'product_payments' is not null
		),
second_t as (
		select json_array_elements(product_payments::json) as product_payments
		from first_t
		)
select product_payments::JSON->>'product_name' as product_name
from second_t
group by product_payments::JSON->>'product_name'