CREATE TABLE stg.bonussystem_users (
	id integer NOT null
	,order_user_id text NOT null
	,CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE TABLE stg.bonussystem_ranks  (
	id integer NOT NULL
	,"name" varchar(2048) NOT NULL
	,bonus_percent numeric(19, 5) NOT NULL DEFAULT 0
	,min_payment_threshold numeric(19, 5) NOT NULL DEFAULT 0
	,CONSTRAINT ranks_bonus_percent_check CHECK ((bonus_percent >= (0)::numeric))
	,CONSTRAINT ranks_bonus_percent_check1 CHECK ((bonus_percent >= (0)::numeric)) 
	,CONSTRAINT ranks_pkey PRIMARY KEY (id)
);

CREATE TABLE stg.bonussystem_events  (
	id integer NOT NULL 
	,event_ts timestamp NOT NULL
	,event_type varchar NOT NULL
	,event_value text NOT NULL
	,CONSTRAINT outbox_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_bonussystem_events__event_ts  ON stg.bonussystem_events USING btree (event_ts);




CREATE TABLE stg.ordersystem_orders  (
	id serial NOT NULL
	,object_id  varchar NOT NULL
	,object_value text NOT NULL
	,update_ts  timestamp NOT NULL
	,CONSTRAINT orders_pkey PRIMARY KEY (id)
);

CREATE TABLE stg.ordersystem_restaurants  (
	id serial NOT NULL
	,object_id  varchar NOT NULL
	,object_value text NOT NULL
	,update_ts  timestamp NOT NULL
	,CONSTRAINT restaurants_pkey PRIMARY KEY (id)
);

CREATE TABLE stg.ordersystem_users  (
	id serial NOT NULL
	,object_id  varchar NOT NULL
	,object_value text NOT NULL
	,update_ts  timestamp NOT NULL
	,CONSTRAINT ordersystem_users_pkey PRIMARY KEY (id)
);


ALTER TABLE stg.ordersystem_orders ADD CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id);
ALTER TABLE stg.ordersystem_restaurants ADD CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id);
ALTER TABLE stg.ordersystem_users ADD CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id);
