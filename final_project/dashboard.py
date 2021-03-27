import os
from data_preprocessing import *
from data_analytics import create_world_analytics
import pickle

def compute_merged_df():
    spark = get_spark()

    cases_sdf = spark_load_cases(spark)
    recoveries_sdf = spark_load_recoveries(spark)
    deaths_sdf = spark_load_deaths(spark)
    demographics_sdf = load_demographics(spark)

    date_cols = get_date_cols(cases_sdf, format_date=False)
    countries = get_countries_names(cases_sdf)
    with open('./data/countries_names.pkl', 'wb') as f:
        pickle.dump(countries, f, protocol=pickle.HIGHEST_PROTOCOL)

    cases_wdf = transform_and_clean(cases_sdf, date_cols, data_col_name='cases')
    recoveries_wdf = transform_and_clean(recoveries_sdf, date_cols, data_col_name='recoveries')
    deaths_wdf = transform_and_clean(deaths_sdf, date_cols, data_col_name='deaths')
    print("Creatomg the joined data")
    joined_wdf = merge_covid_data(cases_wdf, recoveries_wdf, deaths_wdf)
    
    return joined_wdf

def clean_and_analyze_covid_data(save_path="./data/"):
    joined_wdf = compute_merged_df()
    
    print("Computing world aggregates for the dashboard")
    create_world_analytics(joined_wdf, save_path=save_path)
    
    print("Saving joined data to disk")
    joined_wdf.repartition(1)\
              .write.format("com.databricks.spark.csv")\
              .option("header", "true")\
              .save(os.path.join(save_path, "joined_data.csv") )
                
