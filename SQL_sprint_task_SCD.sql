CREATE TABLE clients
(
    client_id INTEGER NOT NULL
        CONSTRAINT clients_pk PRIMARY KEY,
    name      TEXT    NOT NULL,
    login     TEXT    NOT NULL
);

CREATE TABLE products
(
    product_id INTEGER        NOT NULL
        CONSTRAINT products_pk PRIMARY KEY,
    name       TEXT           NOT NULL,
    price      NUMERIC(14, 2) NOT NULL
);

CREATE TABLE sales
(
    client_id  INTEGER        NOT NULL
        CONSTRAINT sales_clients_client_id_fk REFERENCES clients,
    product_id INTEGER        NOT NULL
        CONSTRAINT sales_products_product_id_fk REFERENCES products,
    amount     INTEGER        NOT NULL,
    total_sum  NUMERIC(14, 2) NOT NULL,
    CONSTRAINT sales_pk PRIMARY KEY (client_id, product_id)
); 


update public.clients
set login = 'arthur_dent'
where client_id = 42


-- Удалите внешний ключ из sales
alter table public.sales
drop constraint sales_products_product_id_fk;

-- Удалите первичный ключ из products
alter table public.products
drop constraint products_pk;

-- Добавьте новое поле id для суррогантного ключа в products
alter table public.products
add column id serial NOT NULL ;

-- Сделайте данное поле первичным ключом
alter table public.products
add constraint id_pk PRIMARY KEY (id);

-- Добавьте дату начала действия записи в products
alter table public.products
add column valid_from timestamptz not null;

-- Добавьте дату окончания действия записи в products
alter table public.products
add column valid_to  timestamptz;


-- Добавьте новый внешний ключ sales_products_id_fk в sales
alter table public.sales
add constraint sales_products_id_fk foreign key (product_id) REFERENCES products (id);