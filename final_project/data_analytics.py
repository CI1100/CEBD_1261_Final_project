from datetime import datetime, timedelta
import pickle5 as pickle
import pyspark.sql.functions as pyspark_f
from data_preprocessing import get_spark, load_dataset

def load_joined_data(path="./data/"):
    spark = get_spark()
    return load_dataset(spark, path, "joined_data.csv")

def get_col_sum(sdf, col_name):
    return sdf.select(pyspark_f.sum(col_name)).first()[0]

def get_col_max(sdf, col_name):
    return sdf.select(pyspark_f.max(col_name)).first()[0]

def get_stats(sdf):
    res = {}
    res['total_cases'] = get_col_sum(sdf, 'cases')
    res['total_recoveries'] = get_col_sum(sdf, 'recoveries')
    res['total_deaths'] = get_col_sum(sdf, 'deaths')

    res['total_new_cases'] = get_col_sum(sdf, 'new_cases')
    res['total_new_recoveries'] = get_col_sum(sdf, 'new_recoveries')
    res['total_new_deaths'] = get_col_sum(sdf, 'new_deaths')

    return res
    
def get_stats_by_date(sdf, date):
    update_date_df = sdf.filter(sdf['date'] == date)

    return get_stats(update_date_df)


def create_world_analytics(wdf, save_path="./data"):
    update_date = get_col_max(wdf, 'date')
    day_before = update_date - timedelta(days=1)

    today_stats = get_stats_by_date(wdf, update_date)
    yesterday_stats = get_stats_by_date(wdf, day_before)
    deltas_stats = {}
    for k, v in today_stats.items():
        deltas_stats[k] = today_stats[k] - yesterday_stats[k]
        
    world_analytics = {'today':today_stats,
                       'yesterday':yesterday_stats,
                       'deltas':deltas_stats}
    
    with open('./data/world_stats.pkl', 'wb') as f:
        pickle.dump(world_analytics, f, protocol=pickle.HIGHEST_PROTOCOL)
        
        
def run_analytics(joined_wdf):
    create_world_analytics(joined_wdf, save_path="./data")