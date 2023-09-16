create schema dds;

create table dds.dm_users (
	id serial constraint dm_users_pkey primary key not null
	,user_id varchar not null
	,user_name varchar not null 
	,user_login varchar not null
	);
	
create table dds.dm_restaurants (
	id serial constraint dm_restaurants_pkey primary key not null
	,restaurant_id  varchar not null
	,restaurant_name varchar not null
	,active_from timestamp not null
	,active_to timestamp not null
	);
	
drop table dds.dm_products CASCADE;
create table dds.dm_products (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE)
	,restaurant_id integer not null
	,product_id varchar not null
	,product_name varchar not null
	,product_price numeric(14,2) default 0 not null
		constraint dm_products_product_price_check check (product_price >= 0)
	,active_from timestamp not null
	,active_to timestamp not null
	,CONSTRAINT dm_products_pkey PRIMARY KEY (id)
	);	
alter table dds.dm_products
add constraint dm_products_restaurant_id_fkey foreign key (restaurant_id) REFERENCES dds.dm_restaurants (id);
ALTER TABLE dds.dm_products 
ADD CONSTRAINT unique_products_history_check UNIQUE (restaurant_id, product_id, active_to);


drop table dds.dm_timestamps;
create table dds.dm_timestamps (
	id serial constraint dm_timestamps_pkey primary key not null
	,ts timestamp not null
	,year smallint not null
		constraint dm_timestamps_year_check check (year >= 2022 and year < 2500)
	,month smallint not null
		constraint dm_timestamps_month_check check (month >= 1 and month <= 12)
	,day smallint not null
		constraint dm_timestamps_day_check check (day >= 1 and day <= 31)
	,time time not null
	,date date not null
	);

drop table dds.dm_orders CASCADE;
create table dds.dm_orders (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE)
	,user_id integer not null
	,restaurant_id integer not null
	,timestamp_id integer not null
	,order_key varchar not null
	,order_status varchar not null
	,CONSTRAINT dm_orders_pkey PRIMARY KEY (id)
	);
alter table dds.dm_orders
add constraint dm_orders_user_id_fkey foreign key (user_id) REFERENCES dds.dm_users (id);
alter table dds.dm_orders
add constraint dm_orders_restaurant_id_fkey foreign key (restaurant_id) REFERENCES dds.dm_restaurants (id);
alter table dds.dm_orders
add constraint dm_orders_timestamp_id_fkey foreign key (timestamp_id) REFERENCES dds.dm_timestamps (id);
alter table dds.dm_orders
add constraint unique_dm_orders_order_key_check unique (order_key);

drop table dds.fct_product_sales CASCADE;
create table dds.fct_product_sales (
	id serial constraint fct_product_sales_pkey primary key not null
	,product_id integer not null
	,order_id integer not null
	,count  integer not null default 0
		constraint fct_product_sales_count_check check (count >= 0)
	,price numeric(14,2) not null default 0
		constraint fct_product_sales_price_check check (price >= 0)
	,total_sum numeric(14,2) not null default 0
		constraint fct_product_sales_total_sum_check check (total_sum >= 0)
	,bonus_payment numeric(14,2) not null default 0
		constraint fct_product_sales_bonus_payment_check check (bonus_payment >= 0)
	,bonus_grant numeric(14,2) not null default 0
		constraint fct_product_sales_bonus_grant_check check (bonus_grant >= 0)
	);
alter table dds.fct_product_sales
add constraint fct_product_sales_product_id_fkey foreign key (product_id) REFERENCES dds.dm_products (id);
alter table dds.fct_product_sales
add constraint fct_product_sales_order_id_fkey foreign key (order_id) REFERENCES dds.dm_orders (id);
ALTER TABLE dds.fct_product_sales 
ADD CONSTRAINT unique_fct_product_sales_check UNIQUE (product_id, order_id);


drop table public.outbox;
create table public.outbox (
	id int primary key
	,object_id int not null
	,record_ts timestamp not null
	,type varchar not null
	,payload text not null );



