    #!/usr/bin/env python
    # coding: utf-8

    # #### Our idea for the new column is calculating the number of hospitals near the accident (in range 10km ) location to have some insights about the relation between number of casualties and medical facilities around.
    # 
    # #### So, we do web scraping to extract data about hospitals in UK.

    # In[190]:


def hospitals(path):
    import pandas as pd


    # # loading the clean dataset

    # We load the data set in parquet format from the previous milestone

    # In[191]:


    accidents = pd.read_parquet(path)



    # convert easting and northing to longitude and latitude
    # UTMtoLL was found in the (js) source code of: https://www.engineeringtoolbox.com/utm-latitude-longitude-d_1370.html
    # then converted this javascript function to its python equivilant 

    # Here, we realized it's not UTM northing and easting but something called british national grid system 

    # In[194]:


    def BNG_to_LL(east,north):
        import requests
        uri = f"https://webapps.bgs.ac.uk/data/webservices/CoordConvert_LL_BNG.cfc?method=BNGtoLatLng&easting={east}&northing={north}"
        res = requests.get(uri)
        assert(res.status_code ==200)
        body = res.json()
        return str(body["LONGITUDE"])+","+str(body["LATITUDE"])


    # In[195]:




    # That was extremely slow so we used an online website source code that handles BNG to LL conversions in bulks converted from js to python website: https://gridreferencefinder.com/batchConvert/batchConvert.php

    # In[ ]:


    import math


    def Marc (bf0, n, PHI0, PHI):
        return bf0 * (((1 + n + ((5 / 4) * math.pow(n, 2)) + ((5 / 4) * math.pow(n, 3))) * (PHI - PHI0)) - (((3 * n) + (3 * math.pow(n, 2)) + ((21 / 8) * math.pow(n, 3))) * (math.sin(PHI - PHI0)) * (math.cos(PHI + PHI0))) + ((((15 / 8) * math.pow(n, 2)) + ((15 / 8) * math.pow(n, 3))) * (math.sin(2 * (PHI - PHI0))) * (math.cos(2 * (PHI + PHI0)))) - (((35 / 24) * math.pow(n, 3)) * (math.sin(3 * (PHI - PHI0))) * (math.cos(3 * (PHI + PHI0)))))


    def InitialLat (North, n0, afo, PHI0, n, bfo):

        PHI1 = ((North - n0) / afo) + PHI0
        M = Marc(bfo, n, PHI0, PHI1)
        PHI2 = ((North - n0 - M) / afo) + PHI1
        while (abs(North - n0 - M) > 0.00001):
            PHI2 = ((North - n0 - M) / afo) + PHI1
            M = Marc(bfo, n, PHI0, PHI2)
            PHI1 = PHI2
        return PHI2

    def E_N_to_Lat (East, North, a, b, e0, n0, f0, PHI0, LAM0):
        Pi = 3.14159265358979
        RadPHI0 = PHI0 * (Pi / 180)
        RadLAM0 = LAM0 * (Pi / 180)
        af0 = a * f0
        bf0 = b * f0
        e2 = (math.pow(af0, 2) - math.pow(bf0, 2)) / math.pow(af0, 2)
        n = (af0 - bf0) / (af0 + bf0)
        Et = East - e0
        PHId = InitialLat(North, n0, af0, RadPHI0, n, bf0)
        nu = af0 / (math.sqrt(1 - (e2 * (math.pow(math.sin(PHId), 2)))))
        rho = (nu * (1 - e2)) / (1 - (e2 * math.pow(math.sin(PHId), 2)))
        eta2 = (nu / rho) - 1
        VII = (math.tan(PHId)) / (2 * rho * nu)
        VIII = ((math.tan(PHId)) / (24 * rho * math.pow(nu, 3))) * (5 + (3 * (math.pow(math.tan(PHId), 2))) + eta2 - (9 * eta2 * (math.pow(math.tan(PHId), 2))))
        IX = ((math.tan(PHId)) / (720 * rho * math.pow(nu, 5))) * (61 + (90 * ((math.tan(PHId)) ** 2)) + (45 * (math.pow(math.tan(PHId), 4))))
        E_N_to_Lat = (180 / Pi) * (PHId - (math.pow(Et, 2) * VII) + (math.pow(Et, 4) * VIII) - ((Et ** 6) * IX))
        return ( E_N_to_Lat) 


    def E_N_to_Long (East, North, a, b, e0, n0, f0, PHI0, LAM0):

        Pi = 3.14159265358979
        RadPHI0 = PHI0 * (Pi / 180)
        RadLAM0 = LAM0 * (Pi / 180)
        af0 = a * f0
        bf0 = b * f0
        e2 = (math.pow(af0, 2) - math.pow(bf0, 2)) / math.pow(af0, 2)
        n = (af0 - bf0) / (af0 + bf0)
        Et = East - e0
        PHId = InitialLat(North, n0, af0, RadPHI0, n, bf0)
        nu = af0 / (math.sqrt(1 - (e2 * (math.pow(math.sin(PHId), 2)))))
        rho = (nu * (1 - e2)) / (1 - (e2 * math.pow(math.sin(PHId), 2)))
        eta2 = (nu / rho) - 1
        X = (math.pow(math.cos(PHId), -1)) / nu
        XI = ((math.pow(math.cos(PHId), -1)) / (6 * math.pow(nu, 3))) * ((nu / rho) + (2 * (math.pow(math.tan(PHId), 2))))
        XII = ((math.pow(math.cos(PHId), -1)) / (120 * math.pow(nu, 5))) * (5 + (28 * (math.pow(math.tan(PHId), 2))) + (24 * (math.pow(math.tan(PHId), 4))))
        XIIA = ((math.pow(math.cos(PHId), -1)) / (5040 * math.pow(nu, 7))) * (61 + (662 * (math.pow(math.tan(PHId), 2))) + (1320 * (math.pow(math.tan(PHId), 4))) + (720 * (math.pow(math.tan(PHId), 6))))
        E_N_to_Long = (180 / Pi) * (RadLAM0 + (Et * X) - (math.pow(Et, 3) * XI) + (math.pow(Et, 5) * XII) - (math.pow(Et, 7) * XIIA))
        return E_N_to_Long

    def Lat_Long_H_to_X (PHI, LAM, H, a, b):


        Pi = 3.14159265358979
        RadPHI = PHI * (Pi / 180)
        RadLAM = LAM * (Pi / 180)
        e2 = (math.pow(a, 2) - math.pow(b, 2)) / math.pow(a, 2)
        V = a / (math.sqrt(1 - (e2 * (math.pow(math.sin(RadPHI), 2)))))
        return (V + H) * (math.cos(RadPHI)) * (math.cos(RadLAM))

    def Lat_Long_H_to_Y (PHI, LAM, H, a, b):


        Pi = 3.14159265358979
        RadPHI = PHI * (Pi / 180)
        RadLAM = LAM * (Pi / 180)
        e2 = (math.pow(a, 2) - math.pow(b, 2)) / math.pow(a, 2)
        V = a / (math.sqrt(1 - (e2 * (math.pow(math.sin(RadPHI), 2)))))
        return (V + H) * (math.cos(RadPHI)) * (math.sin(RadLAM))

    def Lat_H_to_Z (PHI, H, a, b):

        Pi = 3.14159265358979
        RadPHI = PHI * (Pi / 180)
        e2 = (math.pow(a, 2) - math.pow(b, 2)) / math.pow(a, 2)
        V = a / (math.sqrt(1 - (e2 * (math.pow(math.sin(RadPHI), 2)))))
        return ((V * (1 - e2)) + H) * (math.sin(RadPHI))

    def Helmert_X (X, Y, Z, DX, Y_Rot, Z_Rot, s):


        Pi = 3.14159265358979
        sfactor = s * 0.000001
        RadY_Rot = (Y_Rot / 3600) * (Pi / 180)
        RadZ_Rot = (Z_Rot / 3600) * (Pi / 180)
        return ( X + (X * sfactor) - (Y * RadZ_Rot) + (Z * RadY_Rot) + DX) 

    def Helmert_Y (X, Y, Z, DY, X_Rot, Z_Rot, s):


        Pi = 3.14159265358979
        sfactor = s * 0.000001
        RadX_Rot = (X_Rot / 3600) * (Pi / 180)
        RadZ_Rot = (Z_Rot / 3600) * (Pi / 180)
        return (X * RadZ_Rot) + Y + (Y * sfactor) - (Z * RadX_Rot) + DY

    def Helmert_Z (X, Y, Z, DZ, X_Rot, Y_Rot, s):

        Pi = 3.14159265358979
        sfactor = s * 0.000001
        RadX_Rot = (X_Rot / 3600) * (Pi / 180)
        RadY_Rot = (Y_Rot / 3600) * (Pi / 180)
        return (-1 * X * RadY_Rot) + (Y * RadX_Rot) + Z + (Z * sfactor) + DZ



    def Iterate_XYZ_to_Lat (a, e2, PHI1, Z, RootXYSqr):


        V = a / (math.sqrt(1 - (e2 * math.pow(math.sin(PHI1), 2))))
        PHI2 = math.atan2((Z + (e2 * V * (math.sin(PHI1)))), RootXYSqr)
        while (abs(PHI1 - PHI2) > 0.000000001) :
            PHI1 = PHI2
            V = a / (math.sqrt(1 - (e2 * math.pow(math.sin(PHI1), 2))))
            PHI2 = math.atan2((Z + (e2 * V * (math.sin(PHI1)))), RootXYSqr)
        
        return PHI2

    def XYZ_to_Lat (X, Y, Z, a, b):

        RootXYSqr = math.sqrt(math.pow(X, 2) + math.pow(Y, 2))
        e2 = (math.pow(a, 2) - math.pow(b, 2)) / math.pow(a, 2)
        PHI1 = math.atan2(Z, (RootXYSqr * (1 - e2)))
        PHI = Iterate_XYZ_to_Lat(a, e2, PHI1, Z, RootXYSqr)
        Pi = 3.14159265358979
        return PHI * (180 / Pi)

    def XYZ_to_Long (X, Y):
        Pi = 3.14159265358979
        return math.atan2(Y, X) * (180 / Pi)

    def convert (eastings,northings):
        height = 0
        lat1 = E_N_to_Lat(eastings, northings, 6377563.396, 6356256.910, 400000, -100000, 0.999601272, 49.00000, -2.00000)
        lon1 = E_N_to_Long(eastings, northings, 6377563.396, 6356256.910, 400000, -100000, 0.999601272, 49.00000, -2.00000)
        x1 = Lat_Long_H_to_X(lat1, lon1, height, 6377563.396, 6356256.910)
        y1 = Lat_Long_H_to_Y(lat1, lon1, height, 6377563.396, 6356256.910)
        z1 = Lat_H_to_Z(lat1, height, 6377563.396, 6356256.910)
        x2 = Helmert_X(x1, y1, z1, 446.448, 0.2470, 0.8421, -20.4894)
        y2 = Helmert_Y(x1, y1, z1, -125.157, 0.1502, 0.8421, -20.4894)
        z2 = Helmert_Z(x1, y1, z1, 542.060, 0.1502, 0.2470, -20.4894)
        latitude = XYZ_to_Lat(x2, y2, z2, 6378137.000, 6356752.313)
        longitude = XYZ_to_Long(x2, y2)
        return (latitude, longitude)


    # it was run on a remote server and exported as csv

    # Here, we add longitude and latitude (They were all missing) to our data set as it will ease our calculations later on.

    # In[197]:


    tmp = pd.read_csv("/opt/airflow/dags/files/coordinates conversion.csv")
    print(tmp.head())
    accidents['longitude'] = tmp['long'].values
    accidents['latitude'] = tmp['lat'].values
    accidents.head()


    # Here comes the hospitals data collection part. The code is explained as comments below.

    # In[198]:


    import re
    import requests
    from bs4 import BeautifulSoup

    list_link_england = "https://en.m.wikipedia.org/wiki/List_of_hospitals_in_England"
    list_link_wales = "https://en.m.wikipedia.org/wiki/List_of_hospitals_in_Wales"

    # get the page where all the hospitals are listed 
    page_res = requests.get(list_link_england)

    # make sure it was a successful request (status code 200)
    assert(page_res.status_code == 200)

    # get the response body (the html file)
    page = page_res.content

    # parse the html file
    soup = BeautifulSoup(page, 'html.parser')

    # find all links in the page that's title contained hospital or medical centre or infirmary
    tags_to_hospitals = soup.find_all('a', {'title': re.compile(r'Hospital|Medical Centre|Infirmary')})
    links_to_hospitals = []

    # append all the links to the list of hospital links
    for link in tags_to_hospitals:
        links_to_hospitals.append("https://en.m.wikipedia.org"+link.get('href'))

    print(len(links_to_hospitals))


    # In[199]:


    def get_hospital_details(link_to_hospital:str):
        # get the html of the hospital page
        page_res = requests.get(link_to_hospital)

        # make sure the status code was 200 (successful)
        assert(page_res.status_code == 200)

        # get the body of the response (the html file)
        page = page_res.content

        # parse the html file
        soup = BeautifulSoup(page, 'html.parser')

        # info_card is the famous wikipedia right side bar with the quick info table it has its unique 
        # css class which we used to id the html element
        info_card  = soup.find(class_="infobox vcard")

        return info_card


    # Here, we extract the info out of the HTML code.

    # In[200]:


    # parse the coordinate of a hospital given the info_card of it's html page
    def get_coordinates(info_card):
        try:
            geo_data = info_card.find(string="Coordinates").find_parent().find_parent().find_parent()
            lng = geo_data.find(class_="infobox-data").find(class_="longitude").contents[0]
            lat = geo_data.find(class_="infobox-data").find(class_="latitude").contents[0]
            return lng ,lat
        except:
            return -200,-200

    # parse the founding year of a hospital given the info card of it's html page
    def get_founding_year(info_card) -> int:
        try:
            data = info_card.find(string="Opened").find_parent().find_parent()
            opened = data.find(class_="infobox-data").contents[0]
            return opened
        except:
            return -1
        
    #  parse number of beds
    def get_number_of_beds(info_card) -> int:
        try:
            data = info_card.find(string="Beds").find_parent().find_parent()
            Beds = data.find(class_="infobox-data").contents[0]
            return Beds
        except:
            return 0


    # We creat a dataframe of all hospitals' details to ease our access to them

    # In[201]:


    df = pd.DataFrame(columns=['lat','lng','year','beds'])
    for i in range(len(links_to_hospitals)):
        info = get_hospital_details(links_to_hospitals.pop())
        lng, lat = get_coordinates(info)
        year = get_founding_year(info)
        beds = get_number_of_beds(info)
        df = df.append({'lat':lat ,'lng':lng,'year':year , 'beds':beds}, ignore_index=True)



    # Year column may have text envolved so, we extract the year as integer.

    # In[202]:


    df['year'] = df['year'].str.extract('([0-9]+)')
    df.year.isna().value_counts()


    # In[203]:


    # df = df.dropna(subset= ['year'])
    df.year.isna().value_counts()


    # We decided not to remove hospitals whose year is missing because we verified the hospitals from NHS website (healthcare website in UK) and figured out that hospitals with missing year are older than 1991 but their year of construction was not recorded.

    # In[204]:


    df.year = df.year.fillna(0)    # in order to be able to compare years.
    df_years_ready = df.copy()
    #df_years_ready.head()
    df_years_ready.year.isna().value_counts()


    # In[205]:


    df_years_ready.year = df_years_ready.year.astype('int')
    df_years_ready = df_years_ready[df_years_ready.year <= 1991]
    df_years_ready.year.dtype


    # All recent hospitals (after 1991) are removed.

    # In[206]:


    df_years_ready.head()


    # All hospitals with missing location are removed.

    # In[207]:


    df_lng_lat_clean = df_years_ready.drop(df_years_ready.index[df_years_ready['lng'] == -200])
    len(df_lng_lat_clean)


    # In[208]:


    accidents.head()


    # These following functions parse longitude and latitude from (degrees, minutes, and secoonds) into (decimal) format.
    # The function takes longitude or latitude and returns the result
    # 

    # In[209]:


    import re
    def dms2dd(degrees, minutes, seconds, direction):
        dd = float(degrees) + float(minutes)/60 + float(seconds)/(60*60);
        if direction == 'S' or direction == 'W':
            dd *= -1
        return dd;




    def parse_dms2(dms) :
        parts = re.split("[^\d\.?\w]+", dms)
        if (len(parts) == 3 )  :
            value = dms2dd(parts[0], parts[1], 0 , parts[2])
            
        elif (len(parts) == 2 )  :
            value = dms2dd(parts[0], 0 , 0 , parts[1])


        else:
            value = dms2dd(parts[0], parts[1], parts[2], parts[3])
        
    
        return value   



    # In[210]:


    # this is for testing only
    string = "36°57'9.13' W"
    parse_dms2(string)


    # Creating 2 columns that will contain the new coordinates in decimal format and initializing them with 0.

    # In[212]:


    df_lng_lat_clean['new_lng'] = 0
    df_lng_lat_clean['new_lat'] = 0
    df_lng_lat_clean.head()


    # In[213]:




    # Placing Longitude and Latitude in the new created columns.

    # In[214]:


    for i in range(0,len(df_lng_lat_clean)) :
        df_lng_lat_clean['new_lat'].iloc[i] = parse_dms2(df_lng_lat_clean['lat'].iloc[i] )
        df_lng_lat_clean['new_lng'].iloc[i] = parse_dms2(df_lng_lat_clean['lng'].iloc[i] )
    df_lng_lat_clean.head()


    # This following function calculates the distance between two locations expressed in longitude and latitude.

    # In[215]:


    from math import radians, cos, sin, asin, sqrt

    def haversine(lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance between two points 
        on the earth (specified in decimal degrees)
        """
        # convert decimal degrees to radians 
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        # haversine formula 
        dlon = lon2 - lon1 
        dlat = lat2 - lat1 
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a)) 
        r = 6371 # Radius of earth in kilometers. Use 3956 for miles
        return c * r


    lat1 =  -7.7940023
    lon1 = 110.3656535

    lat2 = -7.79457
    lon2 = 110.36563

    radius = 0.01 # in kilometer

    a = haversine(lon1, lat1, lon2, lat2)

    print('Distance (km) : ', a)
    if a <= radius:
        print('Inside the area')
    else:
        print('Outside the area')


    # In[216]:


    array_of_hospital_counts = []      # this will contain each observation's number of hosppitals around
    count_of_hospitals = 0

    for i in range(0, len(accidents)) :
        count_of_hospitals = 0
        lat1 = accidents['latitude'][i]
        lon1 = accidents['longitude'] [i]      #location of accident

        for j in range(0, len(df_lng_lat_clean)) :
            lat2= df_lng_lat_clean['new_lat'].iloc[j] 
            lon2 = df_lng_lat_clean['new_lng'].iloc[j]      # location of hospital
            distance = haversine(lon1,lat1,lon2, lat2)
            if distance < 10.0 :
                count_of_hospitals +=1
        array_of_hospital_counts .append(count_of_hospitals)


    # In[217]:


    accidents['hospitals_around'] = array_of_hospital_counts
    accidents.head()


    # Now, we start normalizing data once again to be ready for ML model. 

    # In[218]:


    def MinMax_normalisation(col):
        return (col-col.min())/(col.max()-col.min())
    def normalise (df,cols):
        for col in cols:
            df[col] = MinMax_normalisation(df[col])         


    # In[219]:


    data_after_normalization = accidents.copy()
    normalise (data_after_normalization , ['location_easting_osgr' , 'location_northing_osgr' , 'hospitals_around', 'longitude' , 'latitude'])
    data_after_normalization.head()


    # In[220]:


    data_final_form = data_after_normalization.copy()
    # del data_final_form['longitude']
    # del data_final_form['latitude']

    # data_final_form.to_sql

    # In[221]:



    # <a style='text-decoration:none;line-height:16px;display:flex;color:#5B5B62;padding:10px;justify-content:end;' href='https://deepnote.com?utm_source=created-in-deepnote-cell&projectId=63dd09d9-6d35-4543-8296-f68ebe8fca5f' target="_blank">
    # <img alt='Created in deepnote.com' style='display:inline;max-height:16px;margin:0px;margin-right:7.5px;' src='data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPHN2ZyB3aWR0aD0iODBweCIgaGVpZ2h0PSI4MHB4IiB2aWV3Qm94PSIwIDAgODAgODAiIHZlcnNpb249IjEuMSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIiB4bWxuczp4bGluaz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94bGluayI+CiAgICA8IS0tIEdlbmVyYXRvcjogU2tldGNoIDU0LjEgKDc2NDkwKSAtIGh0dHBzOi8vc2tldGNoYXBwLmNvbSAtLT4KICAgIDx0aXRsZT5Hcm91cCAzPC90aXRsZT4KICAgIDxkZXNjPkNyZWF0ZWQgd2l0aCBTa2V0Y2guPC9kZXNjPgogICAgPGcgaWQ9IkxhbmRpbmciIHN0cm9rZT0ibm9uZSIgc3Ryb2tlLXdpZHRoPSIxIiBmaWxsPSJub25lIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiPgogICAgICAgIDxnIGlkPSJBcnRib2FyZCIgdHJhbnNmb3JtPSJ0cmFuc2xhdGUoLTEyMzUuMDAwMDAwLCAtNzkuMDAwMDAwKSI+CiAgICAgICAgICAgIDxnIGlkPSJHcm91cC0zIiB0cmFuc2Zvcm09InRyYW5zbGF0ZSgxMjM1LjAwMDAwMCwgNzkuMDAwMDAwKSI+CiAgICAgICAgICAgICAgICA8cG9seWdvbiBpZD0iUGF0aC0yMCIgZmlsbD0iIzAyNjVCNCIgcG9pbnRzPSIyLjM3NjIzNzYyIDgwIDM4LjA0NzY2NjcgODAgNTcuODIxNzgyMiA3My44MDU3NTkyIDU3LjgyMTc4MjIgMzIuNzU5MjczOSAzOS4xNDAyMjc4IDMxLjY4MzE2ODMiPjwvcG9seWdvbj4KICAgICAgICAgICAgICAgIDxwYXRoIGQ9Ik0zNS4wMDc3MTgsODAgQzQyLjkwNjIwMDcsNzYuNDU0OTM1OCA0Ny41NjQ5MTY3LDcxLjU0MjI2NzEgNDguOTgzODY2LDY1LjI2MTk5MzkgQzUxLjExMjI4OTksNTUuODQxNTg0MiA0MS42NzcxNzk1LDQ5LjIxMjIyODQgMjUuNjIzOTg0Niw0OS4yMTIyMjg0IEMyNS40ODQ5Mjg5LDQ5LjEyNjg0NDggMjkuODI2MTI5Niw0My4yODM4MjQ4IDM4LjY0NzU4NjksMzEuNjgzMTY4MyBMNzIuODcxMjg3MSwzMi41NTQ0MjUgTDY1LjI4MDk3Myw2Ny42NzYzNDIxIEw1MS4xMTIyODk5LDc3LjM3NjE0NCBMMzUuMDA3NzE4LDgwIFoiIGlkPSJQYXRoLTIyIiBmaWxsPSIjMDAyODY4Ij48L3BhdGg+CiAgICAgICAgICAgICAgICA8cGF0aCBkPSJNMCwzNy43MzA0NDA1IEwyNy4xMTQ1MzcsMC4yNTcxMTE0MzYgQzYyLjM3MTUxMjMsLTEuOTkwNzE3MDEgODAsMTAuNTAwMzkyNyA4MCwzNy43MzA0NDA1IEM4MCw2NC45NjA0ODgyIDY0Ljc3NjUwMzgsNzkuMDUwMzQxNCAzNC4zMjk1MTEzLDgwIEM0Ny4wNTUzNDg5LDc3LjU2NzA4MDggNTMuNDE4MjY3Nyw3MC4zMTM2MTAzIDUzLjQxODI2NzcsNTguMjM5NTg4NSBDNTMuNDE4MjY3Nyw0MC4xMjg1NTU3IDM2LjMwMzk1NDQsMzcuNzMwNDQwNSAyNS4yMjc0MTcsMzcuNzMwNDQwNSBDMTcuODQzMDU4NiwzNy43MzA0NDA1IDkuNDMzOTE5NjYsMzcuNzMwNDQwNSAwLDM3LjczMDQ0MDUgWiIgaWQ9IlBhdGgtMTkiIGZpbGw9IiMzNzkzRUYiPjwvcGF0aD4KICAgICAgICAgICAgPC9nPgogICAgICAgIDwvZz4KICAgIDwvZz4KPC9zdmc+' > </img>
    # Created in <span style='font-weight:600;margin-left:4px;'>Deepnote</span></a>
