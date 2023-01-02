def encode(path):
    import pandas as pd
    from sklearn import preprocessing


    ready_to_encode = pd.read_csv(path)

    def label_encode(df,col)->dict:
        label_encoder = preprocessing.LabelEncoder()
        before = df[col].unique()
        df[col] = label_encoder.fit_transform(df[col])
        after= df[col].unique()
        map = zip(before,after)
        return list(map) # type: ignore
    

    mapping = dict()
    trial1 = ready_to_encode.copy()
    ready_to_encode.accident_severity.value_counts()
    ready_to_encode.police_force.value_counts()

    mapping["accident_severity"] = label_encode(trial1,"accident_severity")

    trial1['time'] = pd.to_datetime(trial1['time'], format='%H:%M').dt.hour

    mapping['police_force'] = label_encode(trial1,'police_force')
    trial1['local_authority_district'] = trial1['local_authority_district'].astype(str)
    trial1["local_authority_district"].unique()
    mapping['local_authority_district'] = label_encode(trial1,'local_authority_district')


    trial1['first_road_number'] = trial1['first_road_number'].str.replace("first_road_class is C or Unclassified. These roads do not have official numbers so recorded as zero", '0.0')
    trial1['first_road_number'] = trial1['first_road_number'].astype(float).astype(int)

    trial1['second_road_number'] = trial1['second_road_number'].str.replace("first_road_class is C or Unclassified. These roads do not have official numbers so recorded as zero", '0.0')
    trial1['second_road_number'] = trial1['second_road_number'].str.replace("No Second Road", '-1.0')
    trial1['second_road_number'] = trial1['second_road_number'].astype('float').astype('int')

    for x in ['first_road_class' , 'road_type' , 'junction_detail' , 'junction_control', 'second_road_class' , 'pedestrian_crossing_human_control' , 'pedestrian_crossing_physical_facilities' , 'light_conditions', 'weather_conditions' , 'road_surface_conditions', 'special_conditions_at_site', 'carriageway_hazards',  'day_of_week']:
        print(f'{x}: {trial1[x].value_counts().size} categories')


    def one_hot_encode(df:pd.DataFrame,cols:list):
        to_return = df.copy()
        for column in cols:
            one_hot_encoded = pd.get_dummies(to_return[column], prefix=column , drop_first=True)
            del to_return[column]
            to_return = pd.concat([to_return, one_hot_encoded], axis=1)
            #df = df.join(one_hot_encoded)
        return to_return


    after_encoding = one_hot_encode(trial1, ['first_road_class' , 'road_type' , 'junction_detail' , 'junction_control', 'second_road_class' , 'pedestrian_crossing_human_control' , 'pedestrian_crossing_physical_facilities' , 'light_conditions', 'weather_conditions' , 'road_surface_conditions', 'special_conditions_at_site', 'carriageway_hazards', 'day_of_week'])

    print(f'rows before encoding: {trial1.shape[1]}, rows after encoding: {after_encoding.shape[1]}')


    def mean_normalisation(col):
        return (col-col.mean())/col.std()

    def MinMax_normalisation(col):
        return (col-col.min())/(col.max()-col.min())

    def normalise (df,cols):
        for col in cols:
            df[col] = MinMax_normalisation(df[col])

    normalise(after_encoding,["police_force","local_authority_district","Week_Number","first_road_number","second_road_number","time","number_of_casualties","number_of_vehicles","accident_severity","speed_limit"])
    normalise (after_encoding, ['hospitals_around', 'longitude' , 'latitude'])


    data_set = after_encoding


    import csv
    fields = ['column name', 'label', 'mapping'] 
    with open('lookup', 'w') as f:
        write = csv.writer(f)
        write.writerow(fields)
        write.writerow(['second_road_number',"unclassified",0])
        write.writerow(['second_road_number',"No Second Road",-1])
        write.writerow(['first_road_number',"unclassified",0])
        write.writerow(['first_road_number',"missing",-1])
        for column in mapping:
            values = mapping[column]
            for value in values:
                write.writerow([column ,  value[0], value[1]])

    data_set.to_parquet('/opt/airflow/dags/files/encoded.parquet')