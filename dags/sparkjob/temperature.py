
import os
from datetime import timedelta
from datetime import datetime
from pyspark.sql.functions import col,split,expr,udf,trim,mean,sum,create_map,explode,year
import time
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql import types as T
from pyspark.sql.functions import expr
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType,MapType
import re







"""
       Description:

            This file reads in the land surface temperature data using the
            spark read method and performs data cleaning, processing,
            and finally saves the cleaned data as parquet file to S3.


            spark: spark session
            output_data: the output root directory to the s3 bucket

"""



output_data = "s3a://charlesprojects-sink/"


paramSchema2 = StructType([StructField('dt',DateType()),StructField('AverageTemperature',DoubleType()),
                          StructField( 'AverageTemperatureUncertainty',DoubleType()),
                          StructField('State',StringType()),StructField('Country',StringType())])



landtemp_df = spark.read.csv("s3a://charlesprojects-source/GlobalLandTemperaturesByState.csv",schema=paramSchema2,header=True)

landtemp_df = landtemp_df.where(landtemp_df['Country'] == 'United States')

landtemp_df = landtemp_df.withColumn('statelowercase',F.lower(landtemp_df.State))\
                            .withColumn('year',year(landtemp_df.dt)).drop('State')

landtemp_df = landtemp_df.withColumnRenamed('statelowercase','state')

landtemp_df = landtemp_df.na.drop(subset=['AverageTemperature'])

landtemp_df = landtemp_df.withColumn('id',F.monotonically_increasing_id())

landtemp_df = landtemp_df.select('id','dt','AverageTemperature','AverageTemperatureUncertainty',
                                   'Country','state','year')

# write processed data as parquet file to S3
landtemp_df.write.mode("overwrite").partitionBy("year").parquet(os.path.join(output_data,"landtempData"))
