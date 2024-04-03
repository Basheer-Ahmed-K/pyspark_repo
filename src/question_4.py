from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, explode_outer, posexplode, current_date, year, month, day

spark = SparkSession.builder.appName('spark-assignment').getOrCreate()

json_path = r'C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark assignment\resources\nested_json_file.json'


def read_json(path):
    json_df = spark.read.json(path, multiLine=True)
    return json_df


# 1. Read JSON file provided in the attachment using the dynamic function
print("1. Read JSON file provided in the attachment using the dynamic function")
df = read_json(json_path)
df.printSchema()
df.show(truncate=False)

# 2. flatten the data frame which is a custom schema
print("2. flatten the data frame which is a custom schema")
flatten_json_df = df.select("*", explode("employees").alias("employee")) \
    .select("*", "employee.empId", "employee.empName") \
    .select("*", "properties.name", "properties.storeSize").drop("properties", "employees", "employee")
flatten_json_df.printSchema()
flatten_json_df.show()

# 3. find out the record count when flattened and when it's not flattened(find out the difference why you are getting
# more count)
print("3. find out the record count when flattened and when it's not flattened(find out the difference why you are "
      "getting more count)")
print("\nBefore Flatten: ", end="")
print(df.count())
print("\nAfter Flatten: ", end="")
print(flatten_json_df.count())

# 4. Differentiate the difference using explode, explode outer, posexplode functions
print("4. Differentiate the difference using explode, explode outer, posexplode functions")
data = [
    (1, [1, 2, 3]),
    (2, [4, None, 6]),
    (3, [])
]

# Create DataFrame with custom schema
df = spark.createDataFrame(data, ["id", "numbers"])

# Show original DataFrame
print("Original DataFrame:")
df.show()

# Explode the 'numbers' array column
exploded_df = df.select("id", explode("numbers").alias("number"))
print("Exploded DataFrame:")
exploded_df.show()

# Explode outer the 'numbers' array column
exploded_outer_df = df.select("id", explode_outer("numbers").alias("number"))
print("Exploded Outer DataFrame:")
exploded_outer_df.show()

# PosExplode the 'numbers' array column
pos_exploded_df = df.select("id", posexplode("numbers").alias("pos", "number"))
print("PosExploded DataFrame:")
pos_exploded_df.show()


# 5. Filter the id which is equal to 1001
print("5. Filter the id which is equal to 1001")
flatten_json_df.filter(flatten_json_df['empId'] == 1001).show()

# 6. convert the column names from camel case to snake case
print("6. convert the column names from camel case to snake case")


def toSnakeCase(dataframe):
    for column in dataframe.columns:
        snake_case_col = ''
        for char in column:
            if char.isupper():
                snake_case_col += '_' + char.lower()
            else:
                snake_case_col += char
        dataframe = dataframe.withColumnRenamed(column, snake_case_col)
    return dataframe


snake_case_df = toSnakeCase(flatten_json_df)
snake_case_df.show()


# 7. Add a new column named load_date with the current date
print("7. Add a new column named load_date with the current date")
load_date_df = snake_case_df.withColumn("load_date", current_date())
load_date_df.show()

# 8. create 3 new columns as year, month, and day from the load_date column
print("8. create 3 new columns as year, month, and day from the load_date column")
year_month_day_df = load_date_df.withColumn("year", year(load_date_df.load_date))\
    .withColumn("month", month(load_date_df.load_date))\
    .withColumn("day", day(load_date_df.load_date))
year_month_day_df.show()

