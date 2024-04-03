from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('spark-assignment').getOrCreate()

employee_data = [(11,“james”,” D101”,”ny”,9000,34),
(12,”michel”,” D101”,”ny”,8900,32),
(13,“robert”,” D102”,”ca”,7900,29),
(14,“scott”,” D103”,”ca”,8000,36),
(15,“jen”,” D102”,”ny”,9500,38),
(16,”jeff”,” D103”,”uk”,9100,35),
(17,“maria”,” D101”,”ny”,7900,40)
]