# Pyspark Assignment

## Question 1 

#### Purchase Data
The purchase data consists of two columns: "customer" and "product_model". We have information about which customer bought which product model.
#### Product Data
The product data contains a single column: "product_model". It lists the available product models.
#### Tasks:
1. **Create Dataframe:** create the purchase data and product data dataframes as given in questions
2. **Find Customers who Bought Only iPhone13**: Identify customers who have purchased only the "iPhone13" product model.
3. **Find Customers who Upgraded from iPhone13 to iPhone14**: Determine customers who upgraded from the "iPhone13" product model to the "iPhone14" product model.
4. **Find Customers who Bought All Models in the New Product Data**: Locate customers who have bought all product models listed in the new product data.

## Question 2

1. **Create SparkSession**: Initialize a SparkSession to use PySpark.

2. **Method 1: Create DataFrame using `createDataFrame` function**: - Use the `createDataFrame` function to create a DataFrame from provided data.

3. **Method 2: Read CSV file**: - Use the `credit_cards.csv` function to read credit card data from a CSV file.

4. **Method 3: Read JSON file**: - Use the `credit_cards.json` function to read credit card data from a JSON file.

5. **Partitioning Operations**:
   - Calculate the total number of partitions in the DataFrame using `getNumPartitions`.
   - Increase the partition size by 5 partitions using `repartition`.
   - Decrease the partition size back to its original size.

6. **Masking Credit Card Numbers**:
   - Define a UDF `masked_card_number` to mask the credit card numbers, showing only the last 4 digits.
   - Apply the UDF to the DataFrame column containing credit card numbers.
   - Display the DataFrame with masked credit card numbers.