
import os
from datetime import datetime
from datetime import timedelta
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




paramSchema1 = StructType([StructField('City',StringType()),StructField('State',StringType()),
                          StructField( 'Median Age',DoubleType()),StructField('Male Population',IntegerType()),
                          StructField('Female Population',IntegerType()),StructField('Total Population',IntegerType()),
                          StructField('Number of Veterans',IntegerType()),StructField('Foreign-born',IntegerType()),
                          StructField('Average Household Size',DoubleType()),StructField('State Code',StringType()),
                          StructField('Race',StringType()),StructField('Count',DoubleType())])






"""
      Description:

           This file reads in the population demography data using the
           spark read method and performs data cleaning, processing,
           and finally saves the cleaned data as parquet file to S3.


           spark: spark session
           output_data: the output root directory to the s3 bucket

"""



output_data = "s3a://charlesprojects-sink/"


us_demogr_df = spark.read.csv("s3a://charlesprojects-source/us-cities-demographics.csv", sep=";",schema=paramSchema1, header=True)

us_demogr_df = us_demogr_df.select([(F.lower(us_demogr_df.City)).alias('City'),
                   (F.lower(us_demogr_df.State)).alias('State'),
                   (us_demogr_df['Median Age']).alias('MedianAge'),
                   (us_demogr_df['Male Population']).alias('MalePopulation'),
                   (us_demogr_df['Female Population']).alias('FemalePopulation'),
                   (us_demogr_df['Total Population']).alias('TotalPopulation'),
                   (us_demogr_df['Number of Veterans']).alias('NumberOfVeterans'),
                   (us_demogr_df['Foreign-born']).alias('ForeignBorn'),
                   (us_demogr_df['Average Household Size']).alias('AverageHouseholdSize'),
                   (F.upper(us_demogr_df['State Code'])).alias('StateCode'),'Race'])

cities_demogr_dropna = us_demogr_df.dropna(how ='any')

mean_values = cities_demogr_dropna.select([F.mean('MalePopulation'),F.mean('FemalePopulation'),\
               F.mean('NumberOfVeterans'),F.mean('ForeignBorn'),F.mean('AverageHouseholdSize')]).take(1)

us_demogr_df = us_demogr_df.na.fill({'MalePopulation': mean_values[0]['avg(MalePopulation)'],\
                            'FemalePopulation':mean_values[0]['avg(FemalePopulation)'],\
                            'NumberOfVeterans': mean_values[0]['avg(NumberOfVeterans)'],\
                            'ForeignBorn': mean_values[0]['avg(ForeignBorn)'],\
                            'AverageHouseholdSize': mean_values[0]['avg(AverageHouseholdSize)']})



us_state_demogr = us_demogr_df.groupBy('StateCode','State')\
       .agg({'MedianAge':'mean','MalePopulation':'sum','FemalePopulation':'sum','TotalPopulation':'sum',
            'NumberOfVeterans':'sum','ForeignBorn':'sum','AverageHouseholdSize':'mean'})


for column in us_state_demogr.columns:
    start_index = column.find('(')
    end_index = column.find(')')
    if (start_index and end_index) != -1:
        us_state_demogr = us_state_demogr.withColumnRenamed(column, column[start_index+1:end_index])
    else:
        continue


# write processed data as parquet file to S3
us_state_demogr.write.mode("overwrite").parquet(os.path.join(output_data,"demographyData"))
