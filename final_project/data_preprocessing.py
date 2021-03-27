import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains
from typing import Iterable 
import re
from datetime import datetime
import pandas as pd
import pyspark.sql.functions as pyspark_f
import os

def melt(df: pyspark.sql.DataFrame, 
         id_vars: Iterable[str], value_vars: Iterable[str], 
         var_name: str="variable", 
         value_name: str="value") -> pyspark.sql.DataFrame:
    """
    Source: https://stackoverflow.com/questions/41670103/how-to-melt-spark-dataframe
    Convert :class:`DataFrame` from wide to long format.
    """

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = pyspark_f.array(*(
        pyspark_f.struct(pyspark_f.lit(c).alias(var_name), 
                         pyspark_f.col(c).alias(value_name)) 
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", 
                         pyspark_f.explode(_vars_and_vals))

    cols = id_vars + [
            pyspark_f.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)


def get_spark(appName='covid19_dashboard'):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark


def load_dataset(spark, path, name):
    sdf = spark.read.options(header='True', delimiter=',') \
               .csv(os.path.join(path, name))
    sdf.printSchema()
    return sdf

def spark_load_cases(spark, data_path="./data"):
    fname = "time_series_covid19_confirmed_global.csv"
    return load_dataset(spark, data_path, fname)

def spark_load_recoveries(spark, data_path="./data"):
    fname = "time_series_covid19_recovered_global.csv"
    return load_dataset(spark, data_path, fname)

def spark_load_deaths(spark, data_path="./data"):
    fname = "time_series_covid19_deaths_global.csv"
    return load_dataset(spark, data_path, fname)

def load_demographics(spark, data_path="./data"):
    fname = "UID_ISO_FIPS_LookUp_Table.csv"
    return load_dataset(spark, data_path, fname)

def get_date_cols(sdf, format_date=True):
    date_regex = '(?:[0-9]{1,2}/){1,2}[0-9]{2}'
    columns = sdf.schema.names
    
    date_cols = []
    for col in columns:
        match = re.match(date_regex, col)
        if match:
            if format_date:
                components = col.split("/")
                day = components[1]
                month = components[0]
                year = components[2]
                date_cols.append(f"{year}/{month}/{day}")
            else:
                date_cols.append(col)
    return date_cols


def get_countries_names(sdf):
    return [x['Country/Region'] for x in sdf.select("Country/Region").distinct().collect()]


def add_day_deltas(mdf, col_name):
    w = pyspark.sql.window.Window().partitionBy(col("Country/Region")).orderBy(col("date"))

    return mdf.select("*", 
                     (pyspark_f.col(col_name) - pyspark_f.lag(col_name, 1).over(w))\
                      .alias(f"new_{col_name}")).na.drop()
    
def transform_and_clean(sdf, date_cols, data_col_name):
    mdf = melt(df=sdf, 
               id_vars=['Province/State', 'Country/Region'], 
               value_vars=date_cols, 
               var_name="date", 
               value_name=data_col_name)
    
    # We drop entries that are provincial and we keep only full country data
    #mdf = mdf.filter(mdf['Province/State'].isNull()).drop('Province/State')
    mdf = mdf.withColumn(data_col_name, mdf[data_col_name].cast(IntegerType()) )
    mdf = mdf.groupBy(['Country/Region','date']).agg(pyspark_f.sum(data_col_name).alias(data_col_name))
    
    to_datetime =  pyspark_f.udf(lambda x: datetime.strptime(x, '%m/%d/%y'), 
                                         pyspark.sql.types.DateType())
    
    mdf = mdf.withColumn('date', 
                     to_datetime(pyspark_f.col('date')))
    
    mdf = add_day_deltas(mdf, data_col_name)
    
    return mdf


def merge_covid_data(cases_lsdf, recoveries_lsdf, deaths_lsdf):
    joined_df = cases_lsdf.join(recoveries_lsdf, ['Country/Region', 'date'])\
                          .join(deaths_lsdf, ['Country/Region', 'date'])
    
    return joined_df




