# Pyspark Assignment

## Question 1 


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



## Question 3:
1. **Column Names Update:**
   - The column names in the DataFrame have been updated dynamically to 'log_id', 'user_id', 'user_activity', and 'time_stamp' using a custom function.
   - The function iterates through the existing column names and renames them according to the specified new column names.

2. **Query to Calculate Actions:**
   - A query has been written to calculate the number of actions performed by each user in the last 7 days.
   - The DataFrame is filtered to include only data from the last 7 days, and then grouped by user_id to count the number of actions performed.

3. **Conversion of Timestamp:**
   - The timestamp column has been converted to a new column named 'login_date' with the format YYYY-MM-DD and the data type as Date.
   - This transformation facilitates easier handling and analysis of login date information.

## Question 4:
1. **Read JSON File:**
   - The JSON file provided in the attachment has been read using a dynamic function, allowing for flexibility in reading various JSON file structures.
   - The DataFrame schema is printed and displayed to understand the data structure.

2. **Flatten DataFrame:**
   - The DataFrame has been flattened to a custom schema by using the explode function on nested arrays.
   - The resulting DataFrame contains columns for each nested array element, providing a more structured view of the data.

3. **Record Count Analysis:**
   - The record count before and after flattening the DataFrame has been compared to identify any differences.
   - This analysis helps understand the impact of flattening on the overall record count.

4. **Explode and PosExplode Functions:**
   - Explode, explode outer, and posexplode functions have been applied to a sample DataFrame to understand their differences.
   - Each function is demonstrated with examples and the resulting DataFrames are displayed.

5. **Filtering by ID:**
   - Records with a specific ID value (1001) have been filtered from the DataFrame.
   - This filtering operation retrieves specific rows based on the provided condition.

6. **Convert Column Names:**
   - Column names in camel case have been converted to snake case to maintain consistency and readability.
   - A custom function has been implemented to perform this conversion, and the DataFrame with updated column names is displayed.

7. **Add Load Date Column:**
   - A new column named 'load_date' has been added to the DataFrame, containing the current date for each record.
   - This column provides information about when the data was loaded into the DataFrame.

8. **Create Year, Month, and Day Columns:**
   - From the 'load_date' column, three new columns ('year', 'month', 'day') have been created to extract the corresponding date components.
   - These columns facilitate further analysis and filtering based on specific date attributes.

## Question 5:
1. **Create DataFrames:**
   - Three DataFrames, namely `employee_df`, `department_df`, and `country_df`, have been created with custom schemas defined dynamically.
   - Each DataFrame corresponds to employee data, department data, and country data, respectively.

2. **Average Salary of Each Department:**
   - The average salary of each department has been calculated using the `employee_df` DataFrame.
   - This analysis provides insights into the salary distribution across different departments.

3. **Employees Whose Names Start with 'M':**
   - Employees whose names start with the letter 'M' have been identified along with their respective department names.
   - This filter operation helps in finding specific employee records based on name criteria.

4. **Bonus Calculation:**
   - A new column named 'bonus' has been added to the `employee_df` DataFrame by multiplying the employee's salary by 2.
   - This column represents the bonus amount for each employee.

5. **Reordering Column Names:**
   - The column names of the `employee_df` DataFrame have been reordered as per the specified sequence.
   - This operation facilitates better data organization and readability.

6. **Join Operations:**
   - Inner join, left join, and right join operations have been performed dynamically between the `employee_df` and `department_df` DataFrames.
   - Each join operation yields different results based on the specified join type.

7. **Update State to Country Name:**
   - The 'State' column in the `employee_df` DataFrame has been updated to display country names instead.
   - This transformation enhances the clarity of geographical information in the DataFrame.

8. **Lowercase Column Names and Add Load Date:**
   - All column names in the DataFrame resulting from Question 7 have been converted to lowercase.
   - Additionally, a new column named 'load_date' has been added with the current date, denoting when the data was loaded.
   - These modifications ensure consistency in column naming conventions and enable tracking of data loading timestamps.