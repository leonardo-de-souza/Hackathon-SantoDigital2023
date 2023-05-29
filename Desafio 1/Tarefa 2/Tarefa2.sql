-- Query 1
SELECT products.productname, SUM(sales.orderQuantity) AS total
FROM products
LEFT JOIN productsubcategories ON products.productsubcategorykey = productsubcategories.productsubcategorykey
LEFT JOIN productcategories ON productsubcategories.productcategorykey = productcategories.productcategorykey
LEFT JOIN sales ON products.productkey = sales.productkey
WHERE productcategories.categoryname = 'Bikes'
GROUP BY products.productName
ORDER BY total DESC
LIMIT 10;

-- Query 2
SELECT customers.firstname, customers.lastname, COUNT(sales.ordernumber) AS total
FROM customers
LEFT JOIN sales ON customers.customerkey = sales.customerkey
GROUP BY customers.customerkey, customers.firstname, customers.lastname
ORDER BY total DESC
LIMIT 1;

-- Query 3
SELECT EXTRACT(MONTH FROM sales.orderdate), SUM(products.productprice * sales.orderquantity)
FROM sales
JOIN products ON sales.productkey = products.productkey
GROUP BY EXTRACT(MONTH FROM sales.orderdate)
ORDER BY SUM(products.productprice * sales.orderquantity) DESC
LIMIT 1;

-- Query 4
-- Já que não informações sobre os vendedores, as regiões serão utilizadas
SELECT territories.region, AVG(sales.orderquantity)
FROM sales
JOIN territories ON sales.territorykey = territories.salesterritorykey
WHERE YEAR(sales.orderdate) = 2017
GROUP BY territories.region
ORDER BY AVG(sales.orderquantity) DESC;




