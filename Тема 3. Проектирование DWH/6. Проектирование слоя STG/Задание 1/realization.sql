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