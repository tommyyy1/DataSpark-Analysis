#1 Purchase frequency per customer 
import sqlite3
import pandas as pd

conn = sqlite3.connect('sales_data.db')

query = """
SELECT
    CustomerKey,
    Name,
    COUNT(quantity) AS PurchaseFrequency
FROM
    sales_table
GROUP BY
    CustomerKey, Name
ORDER BY
    PurchaseFrequency DESC;
"""
purchase_frequency = pd.read_sql_query(query, conn)
print(purchase_frequency)

#2 average order value per customer

query = """
SELECT
    CustomerKey,
    Name,
    SUM(quantity * "Unit Price USD") AS total_spent,
    COUNT(DISTINCT "Order Number") AS total_orders,
    SUM(quantity * "Unit Price USD") / COUNT(DISTINCT "Order Number") AS average_order_value
FROM
    sales_table 
GROUP BY
    CustomerKey, Name
ORDER BY
    average_order_value DESC;
"""

average_order_value_per_customer = pd.read_sql_query(query, conn)
print(average_order_value_per_customer)

#3 overall sales performance by country in millions

query = """
SELECT
    Country_y,
    SUM(Quantity * "Unit Price USD") / 1000000 AS TotalSalesMillions
FROM
    sales_table
GROUP BY
    Country_y
ORDER BY
    TotalSalesMillions DESC;
"""

# Execute the query and load the results into a pandas DataFrame
sales_by_country = pd.read_sql_query(query, conn)
print(sales_by_country)

#4 overall sales performance

query = """
SELECT
    SUM(quantity * "Unit Price USD") AS total_revenue,
    AVG(quantity * "Unit Price USD") AS average_order_value,
    COUNT(DISTINCT "Order Number") AS total_orders,
    SUM(quantity) AS total_units_sold
FROM sales_table;
"""

overall_sales_performance = pd.read_sql_query(query, conn)
print(overall_sales_performance)

#5 sales by product
query = """
SELECT
    "Product Name",
    SUM(Quantity) AS TotalQuantitySold,
    SUM(Quantity * "Unit Price USD") AS TotalRevenue
FROM
    sales_table
GROUP BY
    "Product Name"
ORDER BY
    TotalRevenue DESC;
"""

# Execute the query and load the results into a pandas DataFrame
sales_by_product = pd.read_sql_query(query, conn)
print(sales_by_product)

#6 top 10 sales by storekey
query = """
SELECT
    StoreKey,
    SUM(Quantity * "Unit Price USD") AS TotalSales
FROM
    sales_table
GROUP BY
    StoreKey
ORDER BY
    TotalSales DESC
LIMIT 10;
"""

# Execute the query and load the results into a pandas DataFrame
top_10_sales_by_store = pd.read_sql_query(query, conn)
print(top_10_sales_by_store)

#7 most popular product based on sales
query = """
SELECT "Product Name", SUM(Quantity) AS TotalQuantitySold
FROM sales_table
GROUP BY "Product Name"
ORDER BY TotalQuantitySold DESC
LIMIT 5;
"""

# Execute the query and load the results into a pandas DataFrame
most_popular_product = pd.read_sql_query(query, conn)
print(most_popular_product)

#8 least popular product by sales
query = """
SELECT "Product Name", SUM(Quantity) AS TotalQuantitySold
FROM sales_table
GROUP BY "Product Name"
ORDER BY TotalQuantitySold ASC
LIMIT 5;
"""
# Execute the query and load the results into a pandas DataFrame
least_popular_product = pd.read_sql_query(query, conn)
print(least_popular_product)

# Add the 'Sales' column to the sales_table
try:
    cursor.execute("ALTER TABLE sales_table ADD COLUMN Sales DECIMAL(10, 2);")
    conn.commit()
    print("Sales column added successfully.")
except sqlite3.OperationalError as e:
    print(f"Error adding column: {e}")

# Calculate sales and update the table (assuming Quantity and UnitPrice are available)
try:
    cursor.execute("""
    UPDATE sales_table
    SET Sales = Quantity * "Unit Price USD";
    """)
    conn.commit()
    print("Sales column updated successfully.")
except sqlite3.OperationalError as e:
    print(f"Error updating column: {e}")


#9 Evaluate store performance based on sales per sq.M
query = """
SELECT
    StoreKey,
    "Square Meters",
    SUM(Sales) AS TotalSales,
    SUM(Sales) / "Square Meters" AS SalesPerSquareMeter
FROM
    sales_table
GROUP BY
    StoreKey, "Square Meters"
ORDER BY
    SalesPerSquareMeter DESC
    LIMIT 10;
"""

# Execute the query and load the results into a pandas DataFrame
store_performance = pd.read_sql_query(query, conn)
print(store_performance)
 
# 10 Sales by Currency
query = """
SELECT
    "Order Number",
    Currency_x,
    Sales AS OriginalSalesAmount,
    Exchange,
    Sales * Exchange AS SalesAmountInUSD
FROM
    sales_table

ORDER BY
    Currency_x;
"""

currency_impact = pd.read_sql_query(query, conn)
print(currency_impact)
 

# Close the database connection
conn.close()
