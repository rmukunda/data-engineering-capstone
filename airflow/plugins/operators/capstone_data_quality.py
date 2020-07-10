from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *
from pyspark.sql.functions import split

class CapstoneDataQualityOperator(BaseOperator):
    ui_color = '#89DA59'
    template_fields = ['execution_date']
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 warehouse_path="",
                 raw_data_path = "",
                 exec_date = "",
                 *args, **kwargs):

        super(CapstoneDataQualityOperator, self).__init__(*args, **kwargs)
        self.warehouse_path = warehouse_path
        self.execution_date = exec_date

    def create_spark_session(self):
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
            .getOrCreate()
        return spark
        

    def execute(self, context):
        self.log.info(f'Data quality validation for Capstone project')
        spark = self.create_spark_session()
        parquet_list = ([x[0].replace(self.warehouse_path,"") for x in os.walk(self.warehouse_path)][1:])
        for file in parquet_list:
            self.log.info(f'Data quality checks for {file}')
            filepath = self.warehouse_path + file
            records = spark.read.parquet(filepath)
            tup_table = file.replace(".parquet","")
            if (records.count() < 1 or len(records.head(1))==0):
                raise ValueError(f"Data quality check failed. {tup_table} check query returned no results")
            else:
                self.log.info(f"Data quality check for {tup_table} succcessful")
