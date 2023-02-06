# Use urllib to download data to folder
import urllib.request
import pathlib
from io import BytesIO
from zipfile import ZipFile

# obtain project's working directory
print("Enter Project 1 folder Working directory(with no backslash at the end):")
directory = input()

# Data source: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# This is the High Volume For-Hire Vehicle Trip Records link from New York City government website, along with the profile name it will be stored as

# controls which year's data will be downloaded, add interested year number in string if you need more data, as dddd- form
years = ["2019-"]
# set up file names, with dd year prefix, add more years if needed
# only need 2019's data for this project
HVFH_name = ["19HVFH"]
ytaxi_name = ["19ytaxi"]
fileextention = ".parquet"

# controls what range of months of data that will be downloaded. Max at 13 (so it can download december's data)

month = 10


# required data's url without specific time period
HVFH_link = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_"
ytaxi_link = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"

# create new folder for different taxi type


# directory = /~/~/~/~/data/curated/taxi_type/(train or test)
pathlib.Path(directory+"/data/raw/HVFH/train/").mkdir(parents=True, exist_ok=True)
pathlib.Path(directory+"/data/raw/ytaxi/train/").mkdir(parents=True, exist_ok=True)
pathlib.Path(directory+"/data/raw/HVFH/test/").mkdir(parents=True, exist_ok=True)
pathlib.Path(directory+"/data/raw/ytaxi/test/").mkdir(parents=True, exist_ok=True)


# go through all required year's data
for year in years:
    # for 2019's data, we only take data from Feb since it's the month that HVFH data is added
    # if need other month's data, change starting month
    for i in range(2, month):
        # convert i(month) to string
        required_month = str(i)
        
        # required month format is dd, modify month format if month is from 1-9, with length 1
        if len(required_month) == 1:
            final_month = "0"+str(required_month)
        
        
        # finalise final taxi_type monthly data of the year
        HVFH_final_link = HVFH_link+year+final_month+fileextention
        ytaxi_final_link = ytaxi_link+year+final_month+fileextention
        
        for HVFH_filename in HVFH_name:
            # finalise final file name
            HVFH_final_name = final_month+HVFH_filename+fileextention
            
            
            # if it's the last month, it's the test date, store in test
            if i == month - 1:
                urllib.request.urlretrieve(HVFH_final_link, (directory+"/data/raw/HVFH/test/" +HVFH_final_name))
                continue
            # download specific month's data, download directory is /~/~/~/folder_name/data/curated/~taxitype/
            urllib.request.urlretrieve(HVFH_final_link, (directory+"/data/raw/HVFH/train/" +HVFH_final_name))
        
        
        for ytaxi_filename in ytaxi_name:
            # finalise final file name
            ytaxi_final_name = final_month+ytaxi_filename+fileextention
            
            # if it's the last month, it's the test date, store in test
            if i == month - 1:
                urllib.request.urlretrieve(ytaxi_final_link, (directory+"/data/raw/ytaxi/test/" +ytaxi_final_name))
                continue
            # download specific month's data, download directory is /~/~/~/folder_name/data/curated/~taxitype/
            urllib.request.urlretrieve(ytaxi_final_link, (directory+"/data/raw/ytaxi/train/" +ytaxi_final_name))
       


# then download geospatial data, with zip url
zipurl = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip'

# create data path for geospatial data
pathlib.Path(directory+"/data/raw/geospatial/").mkdir(parents=True, exist_ok=True)

# read the zip file, unzip the folder and export files it contains to geospatial folder
with urllib.request.urlopen(zipurl) as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zfile:
        zfile.extractall(directory+"/data/raw/geospatial")



# retrieve taxi_zone look up table in csv, also store in geospatial folder
urllink = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
urllib.request.urlretrieve(urllink, (directory+"/data/raw/geospatial/taxi+_zone_lookup.csv"))

