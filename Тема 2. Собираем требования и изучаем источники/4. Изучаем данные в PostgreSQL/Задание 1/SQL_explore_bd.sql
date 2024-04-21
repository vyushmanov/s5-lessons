select *
from information_schema.columns
where table_schema = 'public'
order by table_name, column_name;

SELECT *
FROM information_schema.constraint_table_usage
WHERE table_schema = 'public'; 

SELECT CONSTRAINT_SCHEMA,
       CONSTRAINT_NAME,
       update_rule,
       delete_rule
FROM information_schema.referential_constraints
WHERE CONSTRAINT_SCHEMA = 'public';



select table_name 
	,column_name 
	,data_type 
	,character_maximum_length 
	,column_default 
	,is_nullable 
from information_schema.columns
where table_schema = 'public'
order by table_name, column_name;