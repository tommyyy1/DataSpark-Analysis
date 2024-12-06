#Read csv file. Sometime the csv file may difficult in opening due to encoding problem. use below code

import pandas as pd
import numpy as np
import mysql.connector

# Try different encodings
encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']

for encoding in encodings:
    try:
        customers = pd.read_csv('Customers.csv', encoding=encoding)
        products = pd.read_csv('Products.csv', encoding=encoding)
        sales = pd.read_csv('Sales.csv', encoding=encoding)
        stores = pd.read_csv('Stores.csv', encoding=encoding)
        exchange_rate = pd.read_csv('Exchange_Rates.csv', encoding=encoding)

        print(f"Successfully read with {encoding} encoding")
        break
    except UnicodeDecodeError:
        print(f"Failed with {encoding} encoding")
        
#check your datatypes
data_types = {
    'Customers': customers.dtypes,
    'Products': products.dtypes,
    'Sales': sales.dtypes,
    'Stores': stores.dtypes,
    'Exchange_Rates': exchange_rate.dtypes,
}

for table_name, dtypes in data_types.items():
    print(f"Data Types for {table_name}:\n{dtypes}\n")

#convert Date into Datetime format. helpful for extracting year, month or day , filtering date ranges
#convert unit cost into numeric
sales['Delivery Date'] = pd.to_datetime(sales['Delivery Date'])
customers['Birthday'] = pd.to_datetime(customers['Birthday'])
sales['Order Date'] = pd.to_datetime(sales['Order Date'])
exchange_rate['Date'] = pd.to_datetime(exchange_rate['Date'])
stores['Open Date'] = pd.to_datetime(stores['Open Date'])

#remove unwanted $ sign in unit price. Keep only numbers and dots
products["Unit Cost USD"] = products["Unit Cost USD"].str.replace(r"[^\d.]", "", regex=True)
products["Unit Price USD"] = products["Unit Price USD"].str.replace(r"[^\d.]", "", regex=True)
products['Unit Cost USD'] = pd.to_numeric(products['Unit Cost USD'], errors='coerce')
products['Unit Price USD'] = pd.to_numeric(products['Unit Price USD'], errors='coerce')

print(sales.dtypes)
print(exchange_rate.dtypes)
print(products.dtypes)
print(stores.dtypes)

#Check for missing values
print("Missing Values Summary:")
print(customers.isnull().sum())
print(products.isnull().sum())
print(sales.isnull().sum())
print(stores.isnull().sum())
print(exchange_rate.isnull().sum())

# Drop rows with any missing values in the 'Sales' DataFrame
#sales.dropna(inplace=True)
sales['date_missing_flag'] = sales[["Delivery Date"]].isnull().any(axis=1).astype(int)
# Forward fill 
sales['Delivery Date'] = sales['Delivery Date'].fillna(method='ffill') # Changed 'obj.ffill' to 'ffill'
# Backward fill
sales['Delivery Date'] = sales['Delivery Date'].fillna(method='bfill') # Changed 'obj.bfill' to 'bfill'
# Drop rows with any missing values in the 'Customers' DataFrame
customers.dropna(subset=["State Code"],inplace=True)
# Drop rows with any missing values in the 'Stores' DataFrame
stores.dropna(subset=["Square Meters"],inplace=True)
# Drop rows with any missing values in the 'exchange_rate' DataFrame
exchange_rate.dropna(inplace=True)

#Check for missing values
print("Missing Values Summary:")
print(customers.isnull().sum())
print(products.isnull().sum())
print(sales.isnull().sum())
print(stores.isnull().sum())
print(exchange_rate.isnull().sum())

print(customers.head())
print(products.head())
print(sales.head())
print(stores.head())
print(exchange_rate.head())

# Drop duplicate rows in each DataFrame
customers.drop_duplicates(inplace=True)
products.drop_duplicates(inplace=True)
sales.drop_duplicates(inplace=True)
stores.drop_duplicates(inplace=True)
exchange_rate.drop_duplicates(inplace=True)

print("Number of duplicates dropped from Customers:", len(customers) )
print("Number of duplicates dropped from Products:", len(products))
print("Number of duplicates dropped from Sales:", len(sales))
print("Number of duplicates dropped from Stores:", len(stores))
print("Number of duplicates dropped from exchange_rate:", len(exchange_rate))

# prompt: re name currency code as  currency in sales.csv

sales.rename(columns={'Currency Code': 'Currency'}, inplace=True)
print(sales.head())

# Merge sales with customers
sales_products = pd.merge(sales, products, on='ProductKey', how='inner')

# Merge with products
sales_products_customers = pd.merge(sales_products, customers, on='CustomerKey', how='inner')

# Merge with stores
sales_all = pd.merge(sales_products_customers, stores, on='StoreKey', how='inner')

#Merge with exchange rates (needs a common date column - adjust as per your data)
sales_all = pd.merge(sales_all, exchange_rate, left_on='Order Date', right_on='Date', how='left')

# Display the first few rows of the merged dataset
print(sales_all.head())

# Assuming 'sales_all' DataFrame is already created as in the previous code.

sales_all.to_csv('merged_sales_data.csv', index=False)

# Assuming 'sales_all' DataFrame is already created as in the previous code.

sales_all.to_csv('merged_sales_data.csv', index=False)

#Store data in sqllite3 instead of mysql
import sqlite3
import pandas as pd

# Establish a connection to the SQLite database (or create one if it doesn't exist)
conn = sqlite3.connect('sales_data.db')


# Read the merged data
try:
    sales_all = pd.read_csv('merged_sales_data.csv')
except FileNotFoundError:
    print("Error: merged_sales_data.csv not found. Please run the data preparation code first.")
    # You might want to exit 
    exit()


# Write the DataFrame to an SQL table
sales_all.to_sql('sales_table', conn, if_exists='replace', index=False)

# Commit the changes and close the connection
conn.commit()
conn.close()

print("Data successfully uploaded to SQLite database.")
