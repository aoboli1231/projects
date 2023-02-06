# Weather website data scraping, data source from https://world-weather.info/
# Extracted date: 2022.8.1
# import BeautifulSoup for html reading, and urllib to access url
from bs4 import BeautifulSoup
import urllib, urllib.request, sys, csv
from datetime import datetime, timedelta
from dateutil import parser
import pathlib
import pandas as pd

# creating two CSV file to be used - each title is separated by a comma, 2-8 as training data, and 9 as testing data
# file_2...file_8 contain Feb to Aug data(for each month), file_9 contain only Sep data

# create folder to store all weather data, read from terminal input
print("Enter Project 1 folder Working directory(with no backslash at the end):")
folder_path = input()

pathlib.Path(folder_path+"/data/raw/weather/csv").mkdir(parents=True, exist_ok=True)
pathlib.Path(folder_path+"/data/raw/weather/parquet").mkdir(parents=True, exist_ok=True)

months = ["Feb", "Mar", "Apr","May","Jun","Jul","Aug","Sep"]

# create new files recording monthly weather data
file_2 = open(folder_path+"/data/raw/weather/csv/weather_2_2019.csv", 'w')
file_3 = open(folder_path+"/data/raw/weather/csv/weather_3_2019.csv", 'w')
file_4 = open(folder_path+"/data/raw/weather/csv/weather_4_2019.csv", 'w')
file_5 = open(folder_path+"/data/raw/weather/csv/weather_5_2019.csv", 'w')
file_6 = open(folder_path+"/data/raw/weather/csv/weather_6_2019.csv", 'w')
file_7 = open(folder_path+"/data/raw/weather/csv/weather_7_2019.csv", 'w')
file_8 = open(folder_path+"/data/raw/weather/csv/weather_8_2019.csv", 'w')
file_9 = open(folder_path+"/data/raw/weather/csv/weather_9_2019.csv", 'w')


file_lst = [file_2, file_3, file_4, file_5, file_6, file_7, file_8, file_9]
for file in file_lst:
    # write column name for each file
    file.write("Date,TMid,Weather,day,end_day"+"\n")


# Note
# All temperature is stored in Fahrenheit


# create list of all weathers extracted from these months
weather_list = []

# create month list to alternate url
month_list = ["february", "march", "april", "may", "june", "july", "august","september"]
# month in num if the month from feb to aug in num form of 2019
month_in_num = ["2019-02-","2019-03-","2019-04-","2019-05-","2019-06-","2019-07-","2019-08-","2019-09-"]

# create public holiday list, from https://www.officeholidays.com/countries/usa/new-york/2019, typed since there are not much public holidays
public_holiday = ["2019-01-01", "2019-01-21", "2019-02-12", "2019-02-18", "2019-05-27", "2019-07-04", "2019-09-02", "2019-10-14", "2019-11-11", "2019-11-28", "2019-12-25"]

# Run through all monthly data, and extract max, min, date and weather condition
for i in range(len(month_list)):
    # the url format of each month's data follows the structure below
    theurl = "https://world-weather.info/forecast/usa/new_york/"+month_list[i]+"-2019/"
    # access the url link with urllib.request.urlopen
    thepage = urllib.request.urlopen(theurl)
    # use BeautifulSoup to access the urllink in html format
    soup = BeautifulSoup(thepage)
    #Locate weather data
    # area where weather data is stored is under < div id="content-left"> </div>
    # <ul class = "ww-month"> </ul> contains boxes of daily weather data, each box is listed as <li> </li>, denote this path as table (of data) even though it is not.
    table = soup.find_all("div", {"id":"content-left"})[0].find_all("ul", {"class":"ww-month"})[0].find_all("li")
    # now go through each day, maximum number of days is 38 as it is the maximum "boxes" on the website
    # create variable day_count to determine monday to sunday
    day_count = 0
    
    # go over all data recorded boxes
    for day in range(0,len(table)):
        day_count = day_count + 1
        # check if box contains data, as it could be empty, convert data to text
        check = table[day].text
        if check == "":
            continue
        
        # if check is not empty, then data is found, extract weather data and write in csv file
        else:
            # get weather type, <i title: weather> </i> contains weather information, stored as title returns Cloudy in html, Partly cloudy, clear etc..
            weather = table[day].find_all("i")[0]['title']
            weather_list.append(weather)
            # get day of the month, stored as <div> day of the month </div> in html
            date = str(table[day].find_all("div")[0].text)
            if len(date) == 1:
                date = "0"+str(date)
            final_date = month_in_num[i]+date
            
            # convert final_date to datetime for date interval calculation
            date_time = parser.parse(final_date)
            # now calculate start time and end time for each sport
            # Sport event start time will be 1.5 hours prior to the game
            end_day = date_time + timedelta(days=1)
            # calculate if it's weekday or weekend, monday to friday as weekday, saturday and sunday as weekend
            # day_count
            which_day = ''
            # if it's less and equal to 5 and greater than 0, it's a weekday (sunday = 7 -> 7*n - n*7 = 0), therefore exclude 0
            if (day_count-day_count//7*7)<=5 and (day_count-day_count//7*7)>0:
                which_day = "0"
            #if >5, then it's weekend
            else:
                which_day = "1"
                
            # get maximum temperature of the day, stored as <span> temp </span> in html, [:-1] to exclude degree sign
            max = int(table[day].find_all("span")[0].text[:-1])
            
            # get minimum temperature of the day, stored as <p> temp </p>, [:-1] to exclude degree sign
            min = int(table[day].find_all("p")[0].text[:-1])
            
            mid = (max+min)/2
            
            
            # check if final_date is public holiday
            if final_date in public_holiday:
                which_day = "1"
            
            #combine these data
            combined = final_date+","+str(mid)+","+weather+","+which_day+","+str(end_day)+"\n"
            
            
            # and write them in csv file, store all month's data seperately for future dataframe join, otherwise join function will be very long due to more iterations for each month
            weather_list.append(weather)
            file_lst[i].write(combined)
            
# finished scraping weather data, close all files
for i in range(len(file_lst)):
    file_lst[i].close()




# open all csv file, and convert weather variables to categorical values
# create weather_type list to set and obtain unique types of weather obtain
weather_type = list(set(weather_list))
df_feb = pd.read_csv(folder_path+"/data/raw/weather/csv/weather_2_2019.csv")
df_mar = pd.read_csv(folder_path+"/data/raw/weather/csv/weather_3_2019.csv")
df_apr = pd.read_csv(folder_path+"/data/raw/weather/csv/weather_4_2019.csv")
df_may = pd.read_csv(folder_path+"/data/raw/weather/csv/weather_5_2019.csv")
df_jun = pd.read_csv(folder_path+"/data/raw/weather/csv/weather_6_2019.csv")
df_jul = pd.read_csv(folder_path+"/data/raw/weather/csv/weather_7_2019.csv")
df_aug = pd.read_csv(folder_path+"/data/raw/weather/csv/weather_8_2019.csv")
df_sep = pd.read_csv(folder_path+"/data/raw/weather/csv/weather_9_2019.csv")

# create list of pandas dataframe to convert column datatype
df_lst =[df_feb, df_mar, df_apr, df_may, df_jun, df_jul, df_aug, df_sep]

# create categorical variable, 0,1,2... to represent weather type
weather_cate_var = []
for i in range(len(weather_type)):
    weather_cate_var.append(i)
    
# convert all dataframe's TMid, weather and day to numeric
for df in df_lst:
    df['Weather'].replace(weather_type,weather_cate_var, inplace=True)
    df['Weather'] = pd.to_numeric(df['Weather'])
    df['TMid'] = pd.to_numeric(df['TMid'])
    df['day'] = pd.to_numeric(df['day'])


# num is used to control the month of the parquet file, here data is obtained starting with feb, hence 2, change num value
# if month is changed
num = 2
# export as parquet form to retain column numeric type
for df in df_lst:
    directory =folder_path+"/data/raw/weather/parquet/weather_" + str(num) +"_2019.parquet"
    df.to_parquet(directory)
    num = num + 1



