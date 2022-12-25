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
    # css class which i used ti id the html element
    info_card  = soup.find(class_="infobox vcard")
    # gets all the data that was in the table but in html tags 
    data_fields = info_card.find_all(class_="infobox-label")  # type: ignore

    return info_card

# parse the coordinate of a hospital given the info_card of it's html page
def get_coordinates(info_card):
    geo_data = info_card.find(string="Coordinates").find_parent().find_parent().find_parent()
    long = geo_data.find(class_="infobox-data").find(class_="longitude").contents[0]
    lat = geo_data.find(class_="infobox-data").find(class_="latitude").contents[0]
    return long ,lat

# parse the founding year of a hospital given the info card of it's html page
def get_founding_year(info_card) -> int:
    data = info_card.find(string="Opened").find_parent().find_parent()
    opened = data.find(class_="infobox-data").contents[0]
    return opened
#  parse number of beds
def get_number_of_beds(info_card) -> int:
    data = info_card.find(string="Beds").find_parent().find_parent()
    Beds = data.find(class_="infobox-data").contents[0]
    return Beds

#  convert easting and northing to longitude and latitude
#  UTMtoLL was found in the (js) source code of: https://www.engineeringtoolbox.com/utm-latitude-longitude-d_1370.html
#  then converted this javascript function to its python equivilant 
def UTM_to_LL(east,north,zone):
    import math
    d = 0.99960000000000004
    d1 = 6378137
    d2 = 0.0066943799999999998

    d4 = (1 - math.sqrt(1-d2))/(1 + math.sqrt(1 - d2))
    d15 = north - 500000
    d16 = east
    d11 = ((zone - 1) * 6 - 180) + 3

    d3 = d2/(1 - d2) 
    d10 = d16 / d 
    d12 = d10 / (d1 * (1 - d2/4 - (3 * d2 *d2)/64 - (5 * math.pow(d2,3))/256)) 
    d14 = d12 + ((3*d4)/2 - (27*math.pow(d4,3))/32) * math.sin(2*d12) + ((21*d4*d4)/16 - (55 * math.pow(d4,4))/32) * math.sin(4*d12) + ((151 * math.pow(d4,3))/96) * math.sin(6*d12) 
    d13 = d14 * 180 / math.pi 
    d5 = d1 / math.sqrt(1 - d2 * math.sin(d14) * math.sin(d14)) 
    d6 = math.tan(d14)*math.tan(d14) 
    d7 = d3 * math.cos(d14) * math.cos(d14) 
    d8 = (d1 * (1 - d2))/math.pow(1-d2*math.sin(d14)*math.sin(d14),1.5) 

    d9 = d15/(d5 * d) 
    lat = d14 - ((d5 * math.tan(d14))/d8)*(((d9*d9)/2-(((5 + 3*d6 + 10*d7) - 4*d7*d7-9*d3)*math.pow(d9,4))/24) + (((61 +90*d6 + 298*d7 + 45*d6*d6) - 252*d3 -3 * d7 *d7) * math.pow(d9,6))/720)  
    lat = lat * 180 / math.pi 
    long = ((d9 - ((1 + 2 * d6 + d7) * math.pow(d9,3))/6) + (((((5 - 2 * d7) + 28*d6) - 3 * d7 * d7) + 8 * d3 + 24 * d6 * d6) * math.pow(d9,5))/120)/math.cos(d14) 
    long = d11 + long * 180 / math.pi 
    return  long , lat

# verifying the data provided we shall compare the number of total number of beds in the fetched data
# to the official numbers provided by the NHS 
# here : https://www.kingsfund.org.uk/publications/nhs-hospital-bed-numbers

# get distance between two point long and lat
def get_distance(long1,lat1,long2,lat2)->float:

    import geopy.distance

    coords_1 = (long1, lat1)
    coords_2 = (long2, lat2)

    return geopy.distance.geodesic(coords_1, coords_2).km

# just testing
print(get_coordinates(get_hospital_details(link_to_hospital=links_to_hospitals.pop())))
