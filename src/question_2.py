from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName('spark-assignment').getOrCreate()

data = [("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)]
schema = ['card_number']

print("Method 1: using createDataFrame function")
credit_card_df = spark.createDataFrame(data, schema)
credit_card_df.show()

print("Method 2: using read csv file")
csv_path = r'C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark assignment\resources\credit_cards.csv'
credit_card_csv_df = spark.read.csv(csv_path, inferSchema=True, header=True)
credit_card_csv_df.show()

print("Method 3: using read json file")
json_path = r'C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark assignment\resources\credit_cards.json'
credit_card_json_df = spark.read.option("multiline", "true").json(json_path)
credit_card_json_df.show()


print("Total No. of partitions: ", end="")
total_partitions = credit_card_df.rdd.getNumPartitions()
print(total_partitions)

print("Increasing partition size by 5 is: ", end="")
new_total_partition = credit_card_df.rdd.repartition(total_partitions + 5)
new_total_partition_size = new_total_partition.getNumPartitions()
print(new_total_partition_size)

print("Decreasing partition size to its original size: ", end="")
original_partition = new_total_partition.repartition(new_total_partition_size - 5)
original_partition_size = original_partition.getNumPartitions()
print(original_partition_size)


def masked_card_number(cardNumber):
    masked_number = '*' * (len(cardNumber) - 4) + cardNumber[-4:]
    return masked_number


masked_card_number_udf = udf(masked_card_number, StringType())
credit_card_df = credit_card_df.withColumn("masked_card_number", masked_card_number_udf(credit_card_df['card_number']))
credit_card_df.show()
