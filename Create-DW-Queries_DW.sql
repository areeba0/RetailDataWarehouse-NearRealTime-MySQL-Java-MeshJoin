-- Step 0: Dropping the existing database if it exists.
DROP DATABASE IF EXISTS MetroDW;

-- Step 1: Creating a new database for the data warehouse.
CREATE DATABASE MetroDW;
USE MetroDW;

-- Step 2: Dropping existing tables if they exist.
-- Removing old tables to avoid schema conflicts and ensure a clean setup.
DROP TABLE IF EXISTS staging_Customers;
DROP TABLE IF EXISTS staging_Products;
DROP TABLE IF EXISTS staging_Transactions;
DROP TABLE IF EXISTS Sales;
DROP TABLE IF EXISTS Customers;
DROP TABLE IF EXISTS Products;
DROP TABLE IF EXISTS Suppliers;
DROP TABLE IF EXISTS Stores;

-- Step 3: Creating the Stores dimension table.
-- This table will hold unique information about stores, including IDs and names.
CREATE TABLE Stores (
    STORE_ID INT PRIMARY KEY, -- Primary key to uniquely identify each store.
    STORE_NAME VARCHAR(255) NOT NULL -- Store name, a required field.
);

-- Step 4: Creating the Suppliers dimension table.
-- This table will store supplier information for products.
CREATE TABLE Suppliers (
    SUPPLIER_ID INT PRIMARY KEY, -- Primary key to uniquely identify each supplier.
    SUPPLIER_NAME VARCHAR(255) NOT NULL -- Supplier name, a required field.
);

-- Step 5: Creating the Products dimension table.
-- This table will hold product details and link them to suppliers and stores.
CREATE TABLE Products (
    PRODUCT_ID INT PRIMARY KEY, -- Primary key to uniquely identify each product.
    PRODUCT_NAME VARCHAR(255) NOT NULL, -- Product name, a required field.
    PRODUCT_PRICE DECIMAL(10, 2) NOT NULL, -- Product price, stored as a decimal for precision.
    SUPPLIER_ID INT, -- Foreign key to link the product to its supplier.
    STORE_ID INT, -- Foreign key to link the product to its store.
    FOREIGN KEY (SUPPLIER_ID) REFERENCES Suppliers(SUPPLIER_ID), -- Establishing a relationship with Suppliers.
    FOREIGN KEY (STORE_ID) REFERENCES Stores(STORE_ID) -- Establishing a relationship with Stores.
);

-- Step 6: Creating the Customers dimension table.
-- This table will store customer information such as name and gender.
CREATE TABLE Customers (
    CUSTOMER_ID INT PRIMARY KEY, -- Primary key to uniquely identify each customer.
    Customer_Name VARCHAR(255) NOT NULL, -- Customer name, a required field.
    Gender VARCHAR(10) NOT NULL -- Gender of the customer (e.g., Male, Female).
);

-- Step 7: Creating the Sales fact table.
-- This table will store transaction data, linking it to customers, products, and stores.
CREATE TABLE Sales (
    Order_ID INT PRIMARY KEY, -- Primary key to uniquely identify each order.
    Order_Date DATETIME NOT NULL, -- Date and time of the order, required for analysis.
    CUSTOMER_ID INT, -- Foreign key linking to the Customers table.
    PRODUCT_ID INT, -- Foreign key linking to the Products table.
    STORE_ID INT, -- Foreign key linking to the Stores table.
    QUANTITY INT NOT NULL, -- Quantity of the product sold, required for calculations.
    TOTAL_SALE DECIMAL(10, 2) NOT NULL, -- Total amount of the sale, calculated for each transaction.
    FOREIGN KEY (CUSTOMER_ID) REFERENCES Customers(CUSTOMER_ID), -- Establishing a relationship with Customers.
    FOREIGN KEY (PRODUCT_ID) REFERENCES Products(PRODUCT_ID), -- Establishing a relationship with Products.
    FOREIGN KEY (STORE_ID) REFERENCES Stores(STORE_ID) -- Establishing a relationship with Stores.
);

-- Step 8: Creating staging tables for raw data.
-- These tables will temporarily hold raw, unprocessed data before cleaning and transformation.

-- Creating the staging_Customers table for raw customer data.
CREATE TABLE staging_Customers (
    CUSTOMER_ID INT, -- Temporary storage for customer ID.
    Customer_Name VARCHAR(255), -- Temporary storage for customer name.
    Gender VARCHAR(255) -- Temporary storage for customer gender.
);

-- Creating the staging_Products table for raw product data.
CREATE TABLE staging_Products (
    PRODUCT_ID INT, -- Temporary storage for product ID.
    PRODUCT_NAME VARCHAR(255), -- Temporary storage for product name.
    PRODUCT_PRICE VARCHAR(255), -- Temporary storage for product price (as string for cleaning).
    SUPPLIER_ID INT, -- Temporary storage for supplier ID.
    SUPPLIER_NAME VARCHAR(255), -- Temporary storage for supplier name.
    STORE_ID INT, -- Temporary storage for store ID.
    STORE_NAME VARCHAR(255) -- Temporary storage for store name.
);

-- Creating the staging_Transactions table for raw transaction data.
CREATE TABLE staging_Transactions (
    Order_ID INT, -- Temporary storage for order ID.
    Order_Date DATETIME, -- Temporary storage for order date.
    PRODUCT_ID INT, -- Temporary storage for product ID.
    QUANTITY INT, -- Temporary storage for quantity sold.
    CUSTOMER_ID INT -- Temporary storage for customer ID.
);

-- Step 9: Loading raw customer data into staging_Customers.
-- Importing customer data from a CSV file into the staging_Customers table.
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/customers_data.csv'
INTO TABLE staging_Customers
FIELDS TERMINATED BY ',' -- Specifying comma as the column separator.
LINES TERMINATED BY '\n' -- Specifying new line as the row separator.
IGNORE 1 ROWS -- Skipping the header row of the CSV file.
(CUSTOMER_ID, Customer_Name, Gender); -- Mapping CSV columns to table fields.

-- Step 10: Loading raw product data into staging_Products.
-- Importing product data from a CSV file while handling potential data cleaning needs.
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/products_data.csv'
INTO TABLE staging_Products
FIELDS TERMINATED BY ',' -- Specifying comma as the column separator.
OPTIONALLY ENCLOSED BY '"' -- Allowing fields to be optionally enclosed by double quotes.
LINES TERMINATED BY '\n' -- Specifying new line as the row separator.
IGNORE 1 ROWS -- Skipping the header row of the CSV file.
(@col1, @col2, @col3, @col4, @col5, @col6, @col7) -- Capturing raw fields into variables.
SET
    PRODUCT_ID = TRIM(@col1), -- Cleaning extra spaces from product ID.
    PRODUCT_NAME = TRIM(@col2), -- Cleaning extra spaces from product name.
    PRODUCT_PRICE = TRIM(@col3), -- Cleaning extra spaces from product price.
    SUPPLIER_ID = TRIM(@col4), -- Cleaning extra spaces from supplier ID.
    SUPPLIER_NAME = TRIM(@col5), -- Cleaning extra spaces from supplier name.
    STORE_ID = TRIM(@col6), -- Cleaning extra spaces from store ID.
    STORE_NAME = TRIM(@col7); -- Cleaning extra spaces from store name.

-- Step 11: Loading raw transaction data into staging_Transactions.
-- Importing transaction data from a CSV file while handling extra columns.
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/transactions.csv'
INTO TABLE staging_Transactions
FIELDS TERMINATED BY ',' -- Specifying comma as the column separator.
OPTIONALLY ENCLOSED BY '"' -- Allowing fields to be optionally enclosed by double quotes.
LINES TERMINATED BY '\n' -- Specifying new line as the row separator.
IGNORE 1 ROWS -- Skipping the header row of the CSV file.
(@col1, @col2, @col3, @col4, @col5, @extra_col) -- Capturing fields, including potential extra columns.
SET
    Order_ID = TRIM(@col1), -- Cleaning extra spaces from order ID.
    Order_Date = STR_TO_DATE(TRIM(@col2), '%Y-%m-%d %H:%i:%s'), -- Parsing and cleaning order date.
    PRODUCT_ID = TRIM(@col3), -- Cleaning extra spaces from product ID.
    QUANTITY = TRIM(@col4), -- Cleaning extra spaces from quantity sold.
    CUSTOMER_ID = TRIM(@col5); -- Cleaning extra spaces from customer ID.

-- Step 12: Inserting data into the Stores table.
-- Populating Stores table with unique store data from the staging_Products table.
INSERT INTO Stores (STORE_ID, STORE_NAME)
SELECT DISTINCT STORE_ID, STORE_NAME
FROM staging_Products
WHERE STORE_ID IS NOT NULL; -- Ignoring null store IDs.

-- Step 13: Inserting data into the Suppliers table.
-- Populating Suppliers table with unique supplier data from staging_Products.
INSERT INTO Suppliers (SUPPLIER_ID, SUPPLIER_NAME)
SELECT
    SUPPLIER_ID,
    MIN(SUPPLIER_NAME) AS SUPPLIER_NAME -- Deduplicating by taking the first alphabetical name for each ID.
FROM staging_Products
WHERE SUPPLIER_ID IS NOT NULL -- Ignoring null supplier IDs.
GROUP BY SUPPLIER_ID; -- Ensuring uniqueness for each supplier ID.

-- Step 14: Inserting data into the Products table.
-- Populating Products table with cleaned product data.
INSERT INTO Products (PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE, SUPPLIER_ID, STORE_ID)
SELECT
    PRODUCT_ID,
    PRODUCT_NAME,
    CAST(REPLACE(PRODUCT_PRICE, '$', '') AS DECIMAL(10, 2)), -- Cleaning and converting price to decimal.
    SUPPLIER_ID,
    STORE_ID
FROM staging_Products;

-- Step 15: Inserting data into the Customers table.
-- Populating Customers table with cleaned customer data.
INSERT INTO Customers (CUSTOMER_ID, Customer_Name, Gender)
SELECT
    CUSTOMER_ID,
    Customer_Name,
    CASE
        WHEN LOWER(TRIM(Gender)) = 'male' THEN 'Male' -- Standardizing gender values to 'Male'.
        WHEN LOWER(TRIM(Gender)) = 'female' THEN 'Female' -- Standardizing gender values to 'Female'.
        ELSE 'Unknown' -- Defaulting unknown values to 'Unknown'.
    END
FROM staging_Customers;

-- Step 16: Inserting data into the Sales table.
-- Populating Sales table with cleaned and calculated transaction data.
INSERT INTO Sales (Order_ID, Order_Date, CUSTOMER_ID, PRODUCT_ID, STORE_ID, QUANTITY, TOTAL_SALE)
SELECT
    t.Order_ID,
    t.Order_Date,
    t.CUSTOMER_ID,
    t.PRODUCT_ID,
    p.STORE_ID,
    t.QUANTITY,
    COALESCE(t.QUANTITY * p.PRODUCT_PRICE, 0) AS TOTAL_SALE -- Calculating total sale with fallback to 0.
FROM
    staging_Transactions t
LEFT JOIN
    Products p ON t.PRODUCT_ID = p.PRODUCT_ID -- Joining with Products to get price and store ID.
WHERE
    t.CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM Customers); -- Ensuring valid customers.

-- Step 17: Validating data by selecting top rows from each table.
SELECT * FROM Customers LIMIT 10;
SELECT * FROM Products LIMIT 10;
SELECT * FROM Suppliers LIMIT 10;
SELECT * FROM Stores LIMIT 10;
SELECT * FROM Sales LIMIT 10;

 -- Q1. Finding Top Revenue-Generating Products on Weekdays and Weekends.
-- Grouping sales by month, day type (weekday or weekend), and product ID.
-- Calculating total revenue for each group and selecting the top 5 products with the highest revenue.
SELECT 
    DATE_FORMAT(Order_Date, '%Y-%m') AS Month, -- Formatting order date into year-month format for grouping.
    CASE 
        WHEN DAYOFWEEK(Order_Date) IN (1, 7) THEN 'Weekend' -- Classifying days as weekends (Sunday or Saturday).
        ELSE 'Weekday' -- Classifying all other days as weekdays.
    END AS Day_Type,
    PRODUCT_ID, -- Selecting product ID for grouping and revenue calculation.
    SUM(TOTAL_SALE) AS Revenue -- Calculating total revenue per group.
FROM Sales
GROUP BY Month, Day_Type, PRODUCT_ID -- Grouping results by month, day type, and product ID.
ORDER BY Revenue DESC -- Sorting by revenue in descending order to find top-performing products.
LIMIT 5; -- Limiting results to the top 5 revenue-generating products.

-- Q2. Calculating Quarterly Revenue Growth Rate for Stores in 2019.
-- Analyzing revenue growth trends quarterly for each store.
WITH QuarterlyRevenue AS (
    SELECT 
        STORE_ID, -- Selecting store ID for grouping.
        QUARTER(Order_Date) AS Quarter, -- Extracting the quarter from the order date.
        SUM(TOTAL_SALE) AS Total_Revenue -- Calculating total revenue per store per quarter.
    FROM Sales
    WHERE YEAR(Order_Date) = 2019 -- Filtering sales data for the year 2019.
    GROUP BY STORE_ID, Quarter -- Grouping by store ID and quarter for revenue calculation.
)
SELECT 
    STORE_ID, -- Displaying store ID.
    Quarter, -- Displaying the quarter.
    Total_Revenue, -- Displaying the total revenue for each quarter.
    ROUND((Total_Revenue - LAG(Total_Revenue) OVER (PARTITION BY STORE_ID ORDER BY Quarter)) / 
    LAG(Total_Revenue) OVER (PARTITION BY STORE_ID ORDER BY Quarter) * 100, 2) AS Growth_Rate -- Calculating the percentage growth rate compared to the previous quarter.
FROM QuarterlyRevenue;

-- Q3. Analyzing Supplier Sales Contribution by Store and Product Category.
-- Providing a detailed breakdown of sales contributions by supplier, store, and product category.
SELECT 
    s.STORE_NAME, -- Displaying store name.
    sp.SUPPLIER_NAME, -- Displaying supplier name.
    p.PRODUCT_NAME AS Product_Category, -- Displaying product name as the product category.
    SUM(sl.TOTAL_SALE) AS Total_Contribution -- Calculating the total sales contribution.
FROM Sales sl
JOIN Products p ON sl.PRODUCT_ID = p.PRODUCT_ID -- Joining with Products table to get product details.
JOIN Stores s ON sl.STORE_ID = s.STORE_ID -- Joining with Stores table to get store details.
JOIN Suppliers sp ON p.SUPPLIER_ID = sp.SUPPLIER_ID -- Joining with Suppliers table to get supplier details.
GROUP BY s.STORE_NAME, sp.SUPPLIER_NAME, Product_Category -- Grouping results by store, supplier, and product category.
ORDER BY s.STORE_NAME, Total_Contribution DESC; -- Sorting by store and contribution in descending order.

-- Q4. Performing Seasonal Analysis of Product Sales.
-- Analyzing sales performance of products across different seasons.
SELECT 
    CASE 
        WHEN MONTH(Order_Date) IN (12, 1, 2) THEN 'Winter' -- Classifying months as Winter.
        WHEN MONTH(Order_Date) IN (3, 4, 5) THEN 'Spring' -- Classifying months as Spring.
        WHEN MONTH(Order_Date) IN (6, 7, 8) THEN 'Summer' -- Classifying months as Summer.
        WHEN MONTH(Order_Date) IN (9, 10, 11) THEN 'Fall' -- Classifying months as Fall.
    END AS Season, -- Labeling the season based on month.
    p.PRODUCT_NAME, -- Displaying product name for grouping and analysis.
    SUM(sl.TOTAL_SALE) AS Total_Sales -- Calculating total sales for each product in each season.
FROM Sales sl
JOIN Products p ON sl.PRODUCT_ID = p.PRODUCT_ID -- Joining with Products table to get product details.
GROUP BY Season, p.PRODUCT_NAME -- Grouping results by season and product.
ORDER BY Season, Total_Sales DESC; -- Sorting by season and sales in descending order.

-- Q5. Calculating Monthly Revenue Volatility for Stores and Suppliers.
-- Analyzing revenue changes between months for each store-supplier pair.
SELECT 
    s.STORE_NAME, -- Displaying store name.
    sp.SUPPLIER_NAME, -- Displaying supplier name.
    DATE_FORMAT(sl.Order_Date, '%Y-%m') AS Month, -- Formatting order date into year-month for grouping.
    IFNULL((SUM(sl.TOTAL_SALE) - LAG(SUM(sl.TOTAL_SALE)) OVER (PARTITION BY s.STORE_NAME, sp.SUPPLIER_NAME ORDER BY DATE_FORMAT(sl.Order_Date, '%Y-%m'))) / 
    LAG(SUM(sl.TOTAL_SALE)) OVER (PARTITION BY s.STORE_NAME, sp.SUPPLIER_NAME ORDER BY DATE_FORMAT(sl.Order_Date, '%Y-%m')) * 100, 0) AS Volatility -- Calculating revenue volatility as a percentage change from the previous month.
FROM Sales sl
JOIN Products p ON sl.PRODUCT_ID = p.PRODUCT_ID -- Joining with Products table to get product details.
JOIN Stores s ON sl.STORE_ID = s.STORE_ID -- Joining with Stores table to get store details.
JOIN Suppliers sp ON p.SUPPLIER_ID = sp.SUPPLIER_ID -- Joining with Suppliers table to get supplier details.
GROUP BY s.STORE_NAME, sp.SUPPLIER_NAME, Month -- Grouping results by store, supplier, and month.
ORDER BY s.STORE_NAME, Month; -- Sorting by store and month for a chronological view.

-- Q6. Identifying Product Affinity - Frequently Bought Together Products.
-- Finding the top 5 pairs of products frequently purchased together by customers.
SELECT 
    p1.PRODUCT_ID AS Product_A, -- First product in the pair.
    p2.PRODUCT_ID AS Product_B, -- Second product in the pair.
    COUNT(*) AS Frequency -- Counting the frequency of these products being bought together.
FROM Sales s1
JOIN Sales s2 ON s1.Order_ID <> s2.Order_ID AND s1.CUSTOMER_ID = s2.CUSTOMER_ID -- Ensuring products are from the same customer but different orders.
JOIN Products p1 ON s1.PRODUCT_ID = p1.PRODUCT_ID -- Joining with Products table for first product.
JOIN Products p2 ON s2.PRODUCT_ID = p2.PRODUCT_ID -- Joining with Products table for second product.
WHERE p1.PRODUCT_ID < p2.PRODUCT_ID -- Avoiding duplicate pairs (e.g., A-B and B-A).
GROUP BY Product_A, Product_B -- Grouping results by product pairs.
ORDER BY Frequency DESC -- Sorting by frequency in descending order.
LIMIT 5; -- Limiting to the top 5 most frequently bought-together pairs.

-- Q7. Performing Yearly Revenue Trends Analysis with ROLLUP.
-- Aggregating yearly revenue by store, supplier, and product, with hierarchical rollup.
SELECT 
    YEAR(s.Order_Date) AS Year, -- Extracting the year from order date.
    COALESCE(st.STORE_NAME, 'Total') AS STORE_NAME, -- Including 'Total' for rollup levels.
    COALESCE(sp.SUPPLIER_NAME, 'Total') AS SUPPLIER_NAME, -- Including 'Total' for rollup levels.
    COALESCE(p.PRODUCT_NAME, 'Total') AS PRODUCT_NAME, -- Including 'Total' for rollup levels.
    SUM(s.TOTAL_SALE) AS Total_Revenue -- Calculating total revenue for each group.
FROM Sales s
JOIN Products p ON s.PRODUCT_ID = p.PRODUCT_ID -- Joining with Products table for product details.
JOIN Suppliers sp ON p.SUPPLIER_ID = sp.SUPPLIER_ID -- Joining with Suppliers table for supplier details.
JOIN Stores st ON s.STORE_ID = st.STORE_ID -- Joining with Stores table for store details.
GROUP BY YEAR(s.Order_Date), st.STORE_NAME, sp.SUPPLIER_NAME, p.PRODUCT_NAME WITH ROLLUP -- Applying rollup for hierarchical aggregation.
ORDER BY YEAR(s.Order_Date), st.STORE_NAME, sp.SUPPLIER_NAME, p.PRODUCT_NAME; -- Sorting for hierarchical display.

-- Q8. Analyzing Half-Yearly Revenue and Quantity for Products.
-- Comparing product performance in the first and second halves of the year.
SELECT 
    p.PRODUCT_NAME, -- Displaying product name for analysis.
    CASE 
        WHEN MONTH(sl.Order_Date) BETWEEN 1 AND 6 THEN 'H1' -- Classifying first half of the year.
        ELSE 'H2' -- Classifying second half of the year.
    END AS Half_Year,
    SUM(sl.TOTAL_SALE) AS Total_Revenue, -- Calculating total revenue for each half-year.
    SUM(sl.QUANTITY) AS Total_Quantity -- Calculating total quantity sold for each half-year.
FROM Sales sl
JOIN Products p ON sl.PRODUCT_ID = p.PRODUCT_ID -- Joining with Products table for product details.
GROUP BY p.PRODUCT_NAME, Half_Year -- Grouping results by product and half-year.
ORDER BY p.PRODUCT_NAME, Half_Year; -- Sorting results for better readability.

-- Q9. Identifying Revenue Spikes and Outliers in Product Sales.
-- Highlighting days with unusually high revenue for each product.
WITH DailyAvg AS (
    SELECT 
        sl.PRODUCT_ID, -- Selecting product ID for grouping.
        DATE(sl.Order_Date) AS Sale_Date, -- Extracting sale date for grouping.
        AVG(sl.TOTAL_SALE) OVER (PARTITION BY sl.PRODUCT_ID) AS Avg_Sale -- Calculating average daily sale for each product.
    FROM Sales sl
)
SELECT 
    p.PRODUCT_NAME, -- Displaying product name for analysis.
    d.Sale_Date, -- Displaying the sale date for analysis.
    d.Avg_Sale, -- Displaying the average sale for the product.
    SUM(sl.TOTAL_SALE) AS Total_Sale, -- Calculating total sale for the day.
    CASE 
        WHEN SUM(sl.TOTAL_SALE) > 2 * d.Avg_Sale THEN 'Outlier' -- Flagging as outlier if sales exceed twice the average.
        ELSE 'Normal' -- Otherwise, marking as normal.
    END AS Spike_Status -- Categorizing the status as 'Outlier' or 'Normal'.
FROM Sales sl
JOIN DailyAvg d ON sl.PRODUCT_ID = d.PRODUCT_ID AND DATE(sl.Order_Date) = d.Sale_Date -- Joining with the daily average results.
JOIN Products p ON sl.PRODUCT_ID = p.PRODUCT_ID -- Joining with Products table for product details.
GROUP BY p.PRODUCT_NAME, d.Sale_Date, d.Avg_Sale -- Grouping by product, sale date, and average sale.
ORDER BY d.Sale_Date, Spike_Status DESC; -- Sorting by date and spike status.

-- Q10. Creating a View for Quarterly Sales Analysis.
-- Optimizing sales analysis with a pre-aggregated view of quarterly sales by store.
CREATE VIEW REGION_STORE_QUARTERLY_SALES AS
SELECT 
    s.STORE_NAME, -- Displaying store name.
    DATE_FORMAT(sl.Order_Date, '%Y') AS Year, -- Extracting year for grouping.
    QUARTER(sl.Order_Date) AS Quarter, -- Extracting quarter for grouping.
    SUM(sl.TOTAL_SALE) AS Total_Sales -- Calculating total sales for each quarter.
FROM Sales sl
JOIN Stores s ON sl.STORE_ID = s.STORE_ID -- Joining with Stores table for store details.
GROUP BY s.STORE_NAME, Year, Quarter -- Grouping results by store, year, and quarter.
ORDER BY s.STORE_NAME, Year, Quarter; -- Sorting results for easy readability.

-- Retrieving data from the created view to verify results.
SELECT * FROM REGION_STORE_QUARTERLY_SALES;















