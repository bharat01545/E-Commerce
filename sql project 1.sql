CREATE database da_project;
use da_project;


# EXTRACT THE DATASET INTO MYSQL by creating table and then extracting 
#the file using 'load data infile' to load the data completely and in a proper way

create table olist_customers_dataset(
customer_id varchar(100),
customer_unique_id varchar(100),
customer_zip_code_prefix int,
customer_city varchar(50),
customer_state varchar(50)
);

load data infile 'C:\\data sets\\E-commers\\olist_customers_dataset.csv'
 into table olist_customers_dataset
fields terminated by ','
ignore 1 lines;

select * from olist_customers_dataset;

create TABLE olist_order_items_dataset(
order_id varchar(50),
order_item_id int,
product_id VARCHAR(50),
seller_id VARCHAR(50),
shipping_limit_date datetime,
price float,
freight_value float
);

load data infile 'C:\\data sets\\E-commers\\olist_order_items_dataset.csv'
 into table olist_order_items_dataset
fields terminated by ','
ignore 1 lines;

select * from olist_order_items_dataset;

CREATE TABLE olist_orders_dataset (
    order_id VARCHAR(255) NOT NULL,
    customer_id TEXT NOT NULL,
    order_status TEXT NOT NULL,
    order_purchase_timestamp DATETIME NOT NULL,
    order_approved_at text,
    order_delivered_carrier_date text,
    order_delivered_customer_date datetime,
    order_estimated_delivery_date DATETIME,
    PRIMARY KEY (order_id)
);
set sql_mode ='';
LOAD DATA INFILE 'C:\\data sets\\E-commers\\olist_orders_dataset.csv'
INTO TABLE olist_orders_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;


CREATE TABLE olist_order_payments_dataset (
    order_id VARCHAR(255) NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(50) NOT NULL,
    payment_installments INT NOT NULL,
    payment_value DOUBLE NOT NULL,
    PRIMARY KEY (order_id, payment_sequential)
);
LOAD DATA INFILE 'C:\\data sets\\E-commers\\olist_order_payments_dataset.csv'
INTO TABLE OLIST_ORDER_PAYMENTS_DATASET
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

SELECT * FROM OLIST_ORDER_PAYMENTS_DATASET;

CREATE TABLE olist_geolocation_dataset (
    geolocation_zip_code_prefix VARCHAR(10) NOT NULL,
    geolocation_lat DOUBLE NOT NULL,
    geolocation_lng DOUBLE NOT NULL,
    geolocation_city VARCHAR(255) NOT NULL,
    geolocation_state VARCHAR(2) NOT NULL);
    
    LOAD DATA INFILE 'C:\\data sets\\E-commers\\olist_geolocation_dataset.csv'
    INTO TABLE OLIST_GEOLOCATION_DATASET
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    IGNORE 1 ROWS;
    
    SELECT * FROM OLIST_GEOLOCATION_DATASET;
    
    
    
    CREATE TABLE olist_order_reviews_dataset (
    review_id VARCHAR(255) NOT NULL,
    order_id VARCHAR(255) NOT NULL,
    review_score int NOT NULL,
    review_comment_title LONGTEXT,
    review_comment_message LONGTEXT,
    review_creation_date text ,
    review_answer_timestamp text
);

    
 LOAD DATA INFILE 'C:\\data sets\\E-commers\\olist_order_reviews_dataset.csv'
    INTO TABLE OLIST_ORDER_REVIEWS_DATASET
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    
    
    CREATE TABLE olist_sellers_dataset (
    seller_id varchar(255) PRIMARY KEY,
    seller_zip_code_prefix TEXT,
    seller_city TEXT,
    seller_state TEXT
);

LOAD DATA INFILE 'C:\\data sets\\E-commers\\olist_sellers_dataset.csv'
    INTO TABLE OLIST_SELLERS_DATASET
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;

CREATE TABLE olist_products_dataset(
  product_id TEXT,
  product_category_name TEXT,
  product_name_length text NULL DEFAULT NULL,
  product_description_length text,
  product_photos_qty text,
  product_weight_g text,
  product_length_cm text,
  product_height_cm text,
  product_width_cm text
);

LOAD DATA INFILE 'C:\\data sets\\E-commers\\olist_products_dataset.csv'
INTO TABLE olist_products_dataset 
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 ROWS;

create table product_category_name_translation(
product_category_name varchar(255),
product_category_name_english varchar(255));

LOAD data infile 'C:\\data sets\\E-commers\\product_category_name_translation.csv'
into table product_category_name_translation
fields terminated by ','
ignore 1 lines;

# KPI1
select if(weekday(olist_orders_dataset.order_purchase_timestamp) in (5,6),'Weekend','Weekday') as Weekend_Weekday,
sum(olist_order_payments_dataset.payment_value) as All_Payment
from olist_orders_dataset inner join
olist_order_payments_dataset on olist_orders_dataset.order_id=olist_order_payments_dataset.order_id group by Weekend_Weekday;

#KPI 2
  SELECT COUNT(*) As REVIEW_5_CREDIT_CARD FROM olist_order_payments_dataset
 INNER JOIN olist_order_reviews_dataset ON olist_order_payments_dataset.order_id = olist_order_reviews_dataset.order_id
 WHERE olist_order_payments_dataset.payment_type = "credit_card" AND olist_order_reviews_dataset.review_score = 5;
 
 -- KPI 3
SELECT olist_products_dataset.product_category_name, ROUND(AVG(DATEDIFF(OLIST_ORDERS_DATASET.ORDER_DELIVERED_CUSTOMER_DATE,OLIST_ORDERS_DATASET.ORDER_PURCHASE_TIMESTAMP)))
AS AVG_DELIVERY_DAYS FROM olist_products_dataset INNER JOIN olist_order_items_dataset ON olist_products_dataset.product_id=olist_order_items_dataset.product_id
INNER JOIN olist_orders_dataset ON olist_order_items_dataset.order_id=olist_orders_dataset.order_id WHERE olist_products_dataset.product_category_name="pet_shop"
GROUP BY olist_products_dataset.product_category_name;


 -- KPI 4
select avg(price) from olist_order_items_dataset join olist_orders_dataset on (olist_order_items_dataset.order_id=olist_orders_dataset.order_id)
join olist_customers_dataset on (olist_orders_dataset.customer_id=olist_customers_dataset.customer_id) where customer_city="sao paulo";

-- KPI 4
select avg(PAYMENT_VALUE) from olist_order_payments_dataset join olist_orders_dataset on (olist_order_payments_dataset.order_id=olist_orders_dataset.order_id)
join olist_customers_dataset on (olist_orders_dataset.customer_id=olist_customers_dataset.customer_id) where customer_city="sao paulo";


-- KPI 5
SELECT 
  olist_order_reviews_dataset.review_score, 
  avg(DATEDIFF(olist_orders_dataset.order_delivered_customer_date, olist_orders_dataset.order_purchase_timestamp)) AS shipping_days 
FROM 
  olist_orders_dataset  
  JOIN olist_order_reviews_dataset
    ON olist_orders_dataset.order_id = olist_order_reviews_dataset.order_id 
WHERE 
  olist_orders_dataset.order_delivered_customer_date IS NOT NULL 
  AND olist_order_reviews_dataset.review_score IS NOT NULL 
GROUP BY 
  olist_order_reviews_dataset.review_score order by olist_order_reviews_dataset.review_score;
