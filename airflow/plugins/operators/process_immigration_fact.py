from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from datetime import datetime
import os
import os.path
from os import path
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *
from pyspark.sql.functions import split

class ProcessImmigrantOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ['execution_date']
    
    
    def __init__(self,
                 load_immigrant_data_path="",
                 save_path="",
                 exec_date="",
                 *args, **kwargs):
        super(ProcessImmigrantOperator, self).__init__(*args, **kwargs)
        self.load_path = load_immigrant_data_path
        self.save_path = save_path
        self.execution_date = exec_date
        
    def getfilename(self):
        exec_dt = self.execution_date
        months = {'01': 'jan',
                 '02': 'feb',
                 '03': 'mar',
                 '04': 'apr',
                 '05': 'may',
                 '06': 'jun',
                 '07': 'jul',
                 '08': 'aug',
                 '09': 'sep',
                 '10': 'oct',
                 '11': 'nov',
                 '12': 'dec'}
        file_name = 'i94_' + months[exec_dt.split("-")[1]] + exec_dt.split("-")[0][2:] + '_sub.sas7bdat'
        return file_name

    def create_spark_session(self):
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
            .enableHiveSupport().getOrCreate()
        return spark    
    
    def execute(self, context):
        sas_file = self.getfilename()
        file_folder = 'immigration_data/' + self.execution_date[:4] + '/'
        file_path = self.load_path + file_folder + sas_file
        if not path.exists(file_path):
            self.log.info('This file {fl} does not exist, quitting processing'.format(fl=file_path))
            return
        self.log.info('Processing Immigration fact load')
        parquet_file = 'immigration_data.parquet'
        spark = self.create_spark_session()
        self.log.info('Spark session loaded')
        
        self.log.info('Loading the immigration data into the dataframe')
        df_spark =spark.read.format('com.github.saurfang.sas.spark').load(file_path)
        df_spark.createOrReplaceTempView("immigration_data")
        self.log.info('data for {month} loaded'.format(month = sas_file.split("_")[1]))
        spark.sql("""
                    select ie.admnum immigration_id, ie.i94res immigrant_country
                        , DATE_ADD('1960-01-01', cast(ie.arrdate as int)) arrival_date
                        ,  DATE_ADD('1960-01-01', cast(ie.depdate as int)) departure_date
                        ,ie.i94port arrival_port, i94addr as destination_state, ie.i94mode as arrival_mode
                        , ie.i94visa as visa_type , ie.visatype  visa_classification
                        , ie.i94bir as age, ie.visapost as visa_issued_at, ie.gender , ie.airline , ie.fltno as flightnum
                        , ie.i94mon arrival_month,ie.i94yr arrival_year
                    from immigration_data ie
                    """).write.partitionBy("arrival_year").mode("append").parquet(self.save_path+parquet_file) 
        self.log.info('Immigration data saved to parquet')
        
        
        
        
    