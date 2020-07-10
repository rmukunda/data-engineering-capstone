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

class ProcessDimensionsOperator(BaseOperator):
    ui_color = '#80BD9E'
    @apply_defaults
    def __init__(self,
                 data_dir = "",
                 save_path = "",
                 mode = "delete",
                 *args, **kwargs):

        super(ProcessDimensionsOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.load_path = data_dir
        self.save_path = save_path
        self.mode = mode
        
    def create_spark_session(self):
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
            .getOrCreate()
        return spark
    
    def process_airlines(self, spark, table_name):
        print("Process airlines data..")
        print("begin process airlines data")
        parquet_file = "airlines.parquet"
        airlines_source = "airlines.csv"
        airlines_df = spark.read.csv(self.load_path+airlines_source,  sep=",", header = True)
        airlines_df.write.mode("overwrite").parquet(self.save_path+parquet_file)
        self.log.info("Processed {table}..".format(table = table_name))

    def process_airports(self, spark, table_name):
        print("Process airports data..")
        print("begin process airports data")
        parquet_file = "airports.parquet"
        airports_source = "airport-codes_csv.csv"
        airports_df = spark.read.csv(self.load_path+airports_source,  sep=",", header = True)
        airports_df.createOrReplaceTempView("airports")
        airports_data = spark.sql("""
            select iata_code, name as airport_name, iso_country country, split(iso_region, '-')[1] state, municipality city
            ,coordinates , type airport_type
            from airports
            where iso_country = 'US'
            and iata_code is not null
            """)
        airports_data.write.mode("overwrite").parquet(self.save_path+parquet_file)
        self.log.info("Processed {table}..".format(table = table_name))
        
        
    def process_country(self, spark, table_name):
        print("Process Country data..")
        print("begin process country data")
        
        country_codes = "country_codes.csv"

        parquet_file = "country.parquet"

        country_df = spark.read.csv(self.load_path+country_codes,  sep="|", header = True, quote = "'")
        #country_df.printSchema()
        country_df.createOrReplaceTempView("country")
        country_table = spark.sql("""select cast(code as int) code, country from country""")
        country_table.write.mode("overwrite").parquet(self.save_path+parquet_file)
        
    def process_state(self, spark, table_name):
        print("Process state data..")
        print("...")
        table_name = "state"
        parquet_file = "state.parquet"
        state_read = "state_codes.csv"
        state_gdp_read = "us_states_gdp.csv"
        state_code_df = spark.read.csv(self.load_path+state_read,  sep="=", header = True, quote = "'")
        state_gdp_df = spark.read.csv(self.load_path+state_gdp_read,  sep=",", header = True)
        state_code_df.createOrReplaceTempView("state_codes")
        state_gdp_df.createOrReplaceTempView("state_gdp")
        state_write = spark.sql("""
                    select sc.state_code, sc.state_name, cast(gp.GDP as int) GDP, cast(gp.gdp_per_capita as int) gdp_per_capita
                    from state_codes sc join state_gdp gp on sc.state_name = upper(trim(gp.state))
                """)
        state_write.write.mode("overwrite").parquet(self.save_path+parquet_file)        
        
    def process_city(self, spark, table_name):
        print("Process city data..")
        print("...")

        parquet_file = "city.parquet"

        print("process temperature data")
        temp = spark.read.csv(self.load_path+"GlobalLandTemperaturesByCity.csv",  sep=",", header = True)
        temp.createOrReplaceTempView("globaltemps")
        us_city_temps = spark.sql("""
                                select city, cast(dt as date), cast(AverageTemperature as float) temperature,'USA' country
                                    from globaltemps where Country = 'United States'
                                    """)
        us_city_temps.createOrReplaceTempView("city_temps")
        spark.sql("""
                                    select city, country
                                    , round(avg(case when month(dt) in (3,4,5) then temperature else null end),1) spring_temp
                                    , round(avg(case when month(dt) in (6,7,8) then temperature else null end),1) summer_temp
                                    , round(avg(case when month(dt) in (9,10,11) then temperature else null end),1) fall_temp
                                    , round(avg(case when month(dt) in (12,1,2) then temperature else null end),1) winter_temp
                                    from city_temps
                                    group by city, country
                                    """).createOrReplaceTempView("seasonal_temp")

        print("processing demographics data")
        demographics = spark.read.csv(self.load_path+"us-cities-demographics.csv", sep = ";", header = True)
        demographics.createOrReplaceTempView("demographics")
        spark.sql("""
                select City city,`State Code` state ,sum(`Total Population`) as population
                from demographics 
                group by City , `State Code`
                """).createOrReplaceTempView("city_demographics")

        city_df = spark.read.option("multiline",True).json(self.load_path+"city_codes.json")
        city_df.createOrReplaceTempView("city")

        spark.sql("""
                select c.city_code, c.city_name city, c.state, s.spring_temp, s.summer_temp, s.fall_temp, s.winter_temp, cd.population
                from city c left join seasonal_temp s on upper(c.city_name) = upper(s.city)
                        left join city_demographics cd on upper(c.city_name) = upper(cd.city) 
                        and upper(c.state)= upper(cd.state)
                        where c.state is not null
                        """).write.mode("overwrite").parquet(self.save_path+parquet_file)

    def execute(self, context):
        #table_name = "airlines"
        dimensions_loaded = int(Variable.get("dimensions_loaded"))
        if dimensions_loaded < 400:
            spark = self.create_spark_session()
        if dimensions_loaded < 100:
            self.process_airlines(spark, 'airlines')
            Variable.set("dimensions_loaded",100)
        if dimensions_loaded < 200:  
            self.process_country(spark, 'country')
            Variable.set("dimensions_loaded",200)
        if dimensions_loaded < 300:
            self.process_state(spark, 'state')
            Variable.set("dimensions_loaded",300)
        if dimensions_loaded < 400:
            self.process_city(spark, 'city')
            Variable.set("dimensions_loaded",400)
        if dimensions_loaded < 500:
            self.process_airports(spark, 'airports')
            Variable.set("dimensions_loaded",500)

        
