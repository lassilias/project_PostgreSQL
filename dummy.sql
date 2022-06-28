select * from results order by pi_db_uid desc limit 10;

CREATE TABLE orders (
	id serial NOT NULL PRIMARY KEY,
	info json NOT null,
	param1 int,
	param2 int,
	param3 int
);

INSERT INTO orders (info, param1, param2,param3)
VALUES('{ "customer": "Lily Bush", "items": {"product": "Diaper","qty": 24}}',1,2, 3),
      ('{ "customer": "Josh William", "items": {"product": "Toy Car","qty": 1}}',4,5, 6),
      ('{ "customer": "Mary Clark", "items": {"product": "Toy Train","qty": 2}}' ,7,8, 9);

CREATE FUNCTION test (info json)
	returns json
language sql  
as 
$$
	select info -> 'items'  AS items 
	from orders
	WHERE info ->> 'customer' like 'Lily Bush' 
$$;