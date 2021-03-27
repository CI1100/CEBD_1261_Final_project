# This file is temporary to replace the need for airflow. It downloads the latest data
# and transforms the data and saves the latest copy for the dashboard to display
#import findspark
#findspark.init()

from dashboard import clean_and_analyze_covid_data
from data_loading import get_JH_github_data

if __name__ == "__main__":
    get_JH_github_data()
    clean_and_analyze_covid_data()