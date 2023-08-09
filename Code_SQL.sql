-- СОЗДАНИЕ ТАБЛИЦЫ

CREATE TABLE IF NOT EXISTS public.product (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    price NUMERIC
)
DISTRIBUTED BY (product_id);


CREATE TABLE IF NOT EXISTS public.sales (
    sales_id SERIAL,
    product_id INT REFERENCES public.product(product_id),
    sales_date DATE,
    cnt INT
)
DISTRIBUTED BY (sales_id)
PARTITION BY RANGE (sales_date)
(
	-- партиция начиная с 1 января 2023 года включительно
	START (DATE '2023-01-01') INCLUSIVE
	-- по 1 августа 2023 НЕ включительно
 	END (DATE '2023-08-01') EXCLUSIVE
 	-- помесячно
	EVERY (INTERVAL '1 month')
);

-- ЗАГРУЗКА ДАННЫХ

INSERT INTO public.product (product_name, price)
VALUES ('телефон', 1050.50), ('кофемашина', 2005.60), ('пылесос', 1499.00),
       ('стиральная машина', 600.05), ('духовая печь', 1022.22);


INSERT INTO public.sales (product_id, sales_date, cnt)
VALUES (1, '2023-01-02', 2), (1, '2023-02-05', 1), (1, '2023-03-15', 2),
       (2, '2023-02-06', 3), (2, '2023-04-10', 3), (2, '2023-05-20', 3),
       (3, '2023-03-05', 1), (3, '2023-04-11', 1), (3, '2023-03-22', 5),
       (4, '2023-01-08', 2), (4, '2023-02-19', 2), (4, '2023-04-01', 1),
       (5, '2023-02-22', 3), (5, '2023-03-12', 2), (5, '2023-01-10', 2);

-- ПЛАН ЗАПРОСА

SET optimizer = on; -- GPORCA оптимизатор
EXPLAIN
SELECT pr.product_name, SUM(pr.price * s.cnt) AS sum_of_sale
FROM sales AS s
JOIN product AS pr ON s.product_id = pr.product_id
-- при таком условии с датами оптимизатор берет конкретную партицию за первый месяц
-- если написать "EXTRACT(MONTH FROM sales_date) = 1" пройдёт по всем партициям!
WHERE pr.product_name = 'телефон' AND sales_date < '2023-02-01'
GROUP BY pr.product_name;