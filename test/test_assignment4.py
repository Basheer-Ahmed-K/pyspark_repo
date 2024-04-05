import unittest

from src.assignment4.util import *


class TestAssignment4(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("spark-assignment").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_created_dataframe(self):
        df = read_json(self.spark, json_path)
        schema = StructType([
            StructField("employees", ArrayType(StructType([
                StructField("empId", IntegerType(), True),
                StructField("empName", StringType(), True)
            ])), True),
            StructField("id", IntegerType(), True),
            StructField("properties", StructType([
                StructField("name", StringType(), True),
                StructField("storeSize", StringType(), True)
            ]), True)
        ])

        # Define data
        data = [
            (
                [
                    (1001, 'Divesh'),
                    (1002, 'Rajesh'),
                    (1003, 'David')
                ],
                1001,
                ("ABC Pvt Ltd", "Medium")
            )
        ]

        expected_df = create_df(self.spark, data, schema)
        self.assertEqual(df.collect(), expected_df.collect())
