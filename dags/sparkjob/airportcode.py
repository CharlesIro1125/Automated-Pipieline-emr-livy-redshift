
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
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType,MapType
from pyspark.sql.functions import expr
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import re



"""
Description:

This file reads the airport code data using the
spark read method and performs data cleaning, processing,
and finally saves the cleaned data as parquet file to S3.


spark: spark session.
output_data: the output root directory to the s3 bucket.


"""


output_data = "s3a://charlesprojects-sink/"

airportCode_df = spark.read.csv("s3a://charlesprojects-source/Airportgeocode.csv",sep = ',',header = True)

airportCode_df = airportCode_df.withColumn('iso_region_state',F.upper(split("iso_region","-")[1])).drop("iso_region").\
            withColumn('ElevationFt',col("elevation_ft").cast('integer')).drop("elevation_ft","_c0")

airportCode_df = airportCode_df.na.fill({'ElevationFt':0,'municipality':'Nil','gps_code':'Nil','iata_code':'Nil',
                        'local_code':'Nil'})

airportCode_df = airportCode_df.dropDuplicates(['ident'])

@udf
def extractCity(line):
    import re
    x = re.search(r"('city':\s*('*\s*\w*(\s*\w*)*\s*'))",line)
    if x:
        x = x.group(2)
        val = x.replace("'","").strip()
    else:
        val = 'Nil'
    return val

airportCode_df = airportCode_df.withColumn("City",extractCity('geocode'))

airportCode_df = airportCode_df.dropDuplicates(['City'])



# write processed data as parquet file to S3
airportCode_df.write.mode("overwrite").parquet(os.path.join(output_data,"airportData"))
