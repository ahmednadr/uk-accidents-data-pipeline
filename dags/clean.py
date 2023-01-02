def clean(path):
    import subprocess
    import sys

    try:
        
        from sklearn.neighbors import LocalOutlierFactor as LOF
    except:
        print("scikit learn installing")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "scikit-learn"])
        
        from sklearn.neighbors import LocalOutlierFactor as LOF

    import pandas as pd
    import numpy as np
    
    data_set = pd.read_csv(path,index_col="accident_index")
    # The dataset starts with 35 columns and 235889 rows, this is prior to any cleaning or encoding

    df_nullrep = data_set.replace({'-1':np.nan, -1:np.nan, 'Data missing or out of range':np.nan})
    clean_df = df_nullrep.dropna(axis='columns',how='all')
    clean_df.isnull().sum().sort_values()

    subs = ['date','time','first_road_number','junction_detail',
            'first_road_class','location_northing_osgr', 'location_easting_osgr', 
            'road_surface_conditions', 'pedestrian_crossing_human_control', 'pedestrian_crossing_physical_facilities', 
            'road_type', 'weather_conditions','light_conditions','special_conditions_at_site','carriageway_hazards']

    clean_df = clean_df.dropna(axis='index', how='any', subset=subs)
    clean_df.isnull().sum()


    print(f'Shape of dataset before: {clean_df.shape[0]}, {len(df_nullrep.columns)}, Shape of dataset after: {df_nullrep.shape[0]}, {len(clean_df.columns)}')

    clean_df['junction_control'] = clean_df['junction_control'].fillna('No Junction')
    clean_df['second_road_class'] = clean_df['second_road_class'].fillna('No Second Road')
    clean_df['second_road_number'] = clean_df['second_road_number'].fillna('No Second Road')

    def observe_outliers (dframe , index_of_column):
        if dframe[dframe.columns[index_of_column]].dtype == 'object':
            pass
        else:
            Q1 = dframe[dframe.columns[index_of_column]].quantile(0.25)
            Q3= dframe[dframe.columns[index_of_column]].quantile(0.75)
            IQR = Q3-Q1
            lower = Q1 - 1.5*IQR
            upper = Q3 + 1.5*IQR
            to_remove = dframe[ (dframe[dframe.columns[index_of_column]] > upper) | (dframe[dframe.columns[index_of_column]] < lower) ]
            to_return = to_remove
            print(len(to_return))
            return to_return

    outliers_removed = clean_df.copy()
    outliers_columns = outliers_removed[['speed_limit', 'number_of_casualties' , 'number_of_vehicles']]
    outliers_columns.head()
    predictions = LOF().fit_predict(outliers_columns)
    outliers_removed['outlier'] = predictions

    outliers_removed = outliers_removed[outliers_removed.outlier == 1]
    del outliers_removed['outlier']

    outliers_columns = outliers_removed[['location_easting_osgr', 'location_northing_osgr' ]]
    outliers_columns.head()
    predictions = LOF().fit_predict(outliers_columns)
    outliers_removed['outlier'] = predictions

    outliers_removed = outliers_removed[outliers_removed.outlier == 1]
    del outliers_removed['outlier']


    week_added = outliers_removed.copy()
    week_added.date = pd.to_datetime(week_added.date)

    week_added['Week_Number'] = week_added['date'].dt.isocalendar().week

    week_added.head()

    ready_to_encode1 = week_added.copy()
    month_num = ready_to_encode1['date'].dt.month
    # season_dict =  dict.fromkeys([1, 2, 12], 'Winter') | dict.fromkeys([3, 4, 5], 'Spring') | dict.fromkeys([6, 7, 8], 'Summer') | dict.fromkeys([9, 10, 11], 'Fall')
    # ready_to_encode1['Season'] = month_num.apply(lambda x: season_dict[x])

    from datetime import time
    ready_to_encode = ready_to_encode1.copy()
    weekday = (ready_to_encode.day_of_week != "Saturday") & (ready_to_encode.day_of_week != "Sunday")
    x = pd.to_datetime(ready_to_encode['time'], format='%H:%M').dt.hour
    rush_hour = ((x.between(16,18) | x.between(7,9)) & weekday).astype(int)
    ready_to_encode['rush_hour'] = rush_hour
    del ready_to_encode['accident_reference']
    del ready_to_encode['accident_year']
    del ready_to_encode['date']

    ready_to_encode.to_parquet('/opt/airflow/dags/files/clean.parquet')


