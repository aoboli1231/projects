# Sports information is scraped from http://www.mustseenewyork.com/new-york-top-sports.html
# Scrap date: 2022.8.2


from bs4 import BeautifulSoup
import urllib, urllib.request, sys, csv
import pandas as pd
import re
from datetime import datetime, timedelta
from dateutil import parser
import pathlib

# obtain project folder directory
print("Enter Project 1 folder Working directory(with no backslash at the end):")
directory = input()

pathlib.Path(directory+ "/data/raw/sport/csv/").mkdir(parents=True, exist_ok=True)

# creating CSVs file to be used - each title is separated by a comma, Feb-Aug as training data, and Sep as testing data (only with date information)
# file_n contain month nth data
file_2 = open(directory+ "/data/raw/sport/csv/sports_2_2019.csv", 'w')
file_3 = open(directory+ "/data/raw/sport/csv/sports_3_2019.csv", 'w')
file_4 = open(directory+ "/data/raw/sport/csv/sports_4_2019.csv", 'w')
file_5 = open(directory+ "/data/raw/sport/csv/sports_5_2019.csv", 'w')
file_6 = open(directory+ "/data/raw/sport/csv/sports_6_2019.csv", 'w')
file_7 = open(directory+ "/data/raw/sport/csv/sports_7_2019.csv", 'w')
file_8 = open(directory+ "/data/raw/sport/csv/sports_8_2019.csv", 'w')
file_9 = open(directory+ "/data/raw/sport/csv/sports_9_2019.csv", 'w')

file_lst = [file_2, file_3, file_4, file_5, file_6, file_7, file_8, file_9]

# write column name for for all files
for file in file_lst:
    file.write("Date,Sport,start_time,end_time,event"+"\n")


# open the url with urllib.request.urlopen, and read the html page through BeautifulSoup
theurl = "http://www.mustseenewyork.com/new-york-top-sports.html"
thepage = urllib.request.urlopen(theurl)
soup = BeautifulSoup(thepage, features="lxml")

# Locate the table that contains sports event date and time (id:table7)
table = soup.find_all("table", {"id":"table7"})[0]

# within the table, each tr is a row of information of date, event, venue(sport type) and tickets
# we are only interested in date and sport type

month = ["2019-02-01 00:00:00", "2019-03-01 00:00:00", "2019-04-01 00:00:00", "2019-05-01 00:00:00",
        "2019-06-01 00:00:00", "2019-07-01 00:00:00", "2019-08-01 00:00:00", "2019-09-01 00:00:00",
        "2019-10-01 00:00:00"]


# Function gets the date and time of the event

def get_date(row):
    # date is on rwo 0
    date = row[0].text
    # remove any new line, tab expression, and replace "/" by "-"
    # use regular expression to obtain useful information
    date = re.sub(r"\n","",date)
    date = re.sub(r"\t","",date)
    # now date is in the format of 'dd/d/dddd dd:dd', rearrange date into dddd-dd-dd dd:dd:dd form
    # split by space to seperate day and time, temp_date[0] is day, temp_date[1] is time
    temp_date = date.split(' ')
    day = temp_date[0].split('/')
    # new check if day and month matches dd form, if not, change to dd form
    # day[0] is month for 2019, and date for 2018, ignore 2018 format
    if len(day[0]) == 1:
        day[0] = "0"+day[0]
    # day[1] is date for 2019, month for 2018, ignore 2018 format
    if len(day[1]) == 1:
        day[1] = "0"+day[1]
        
    # get the start time of game
    start_time = temp_date[1]
    
    # all 13:30 start_time game in this website are typed as 3:30, mannually convert to propertime
    if start_time == "3:30":
            temp_date[1] = "1"+start_time
            
    # now form the final dddd-dd-dd dd:dd:dd date&time
    date = day[2]+'-'+day[0]+'-'+day[1]+ " "+temp_date[1]+":00"
    # return the date
    return date

# Function gets sport's name
def get_sport(row):
    # sport
    sport =row[2].text
    # each row should contain only one sport, as each match is considered as one event, with specific time. Therefore we only need string form of the first of the list
    sport = re.findall(r"SPORTS-{1}[a-zA-Z0-9 ]+",sport)[0]
    # we only need sport's name, starts at 7th of the string, and convert to lowercase for consistency
    sport = sport[7:].lower()
    # return sport's name
    return sport
    


count_2018 = 0
count_2019 = 0
count_10_2019 = 0
event = "1"
# go through each row of data, check for date before
# total number of rows is len(table.find_all("tr"))
# range starts from 1 to skip file headings
for row_num in range(1, len(table.find_all("tr"))):
    # locate row information
    row = table.find_all("tr")[row_num].find_all("font")
    # get the date of the event
    date = get_date(row)
    
    # check if date is between Feb 2019 to Sep 2019
    # if current date is before Feb 2019, skip this event
    if date < month[0]:
        count_2018 = count_2018 + 1
        continue
    # get sport's name
    sport = get_sport(row)

    # convert date to datetime
    date_time = parser.parse(date)
    # now calculate start time and end time for each sport
    # Sport event start time will be 1.5 hours prior to the game
    # allocate pre_game time interval, where fans need to travel to the stadium]
    # making it 1 hour before the game
    pre_game_start_time = date_time - timedelta(hours=1)
    pre_game_end_time = date_time

    # create end time of sport event, different game length for different type of sport
    # type of sports are checked and collected mannually

    end_time = 0
    if sport == "hockey":
        # hockey games last for 1.5 hours, make it to 3 hours for the event to finish
        end_time = date_time + timedelta(hours=1.5)
    if sport == "basketball":
        # basketball games last for 2.5 hours, make it to 3 hours for the event to finish
        end_time = date_time + timedelta(hours=2.5)
    if sport == "wrestling":
        # wrestling games last for 1.5 hours, make it to 2 hours for the event to finish
        end_time = date_time + timedelta(hours=1.5)
    if sport == "baseball":
        # baseball games last for 4 hours, make it to 5 hours for the event to finish
        end_time = date_time + timedelta(hours=4)
    if sport == "tennis":
        # tennis games last for 3 hours, make it to 4 hours for the event to finish
        end_time = date_time + timedelta(hours=3)
        
    # now allocate post game interval, where fans needs to travel back home
    # post game interval starts right after the game ends, make end time 1 hours after the end, as most fans are left
    post_game_start_time = end_time
    post_game_end_time = end_time + timedelta(hours=1)
    
    # data fit into needed time period, with pre game time interval and post game time interval
    pre_combined = date+","+sport+","+str(pre_game_start_time)+","+str(pre_game_end_time)+","+event+"\n"
    post_combined = date+","+sport+","+str(post_game_start_time)+","+str(post_game_end_time)+","+event+"\n"
    
    # now classify this combined data based on Feb to Sep data
    for i in range(len(month)):
        # when date >= dates[i] and < dates[i+1], it means that the date belongs to dates[i] month
        # "2019-03-02 00:00:00" < "2019-04-01 00:00:00", then dates[2] = "2019-04-01 00:00:00"
        # dates[1] = "2019-03-01 00:00:00", with date belongs to march, which is file_3 == file_lst[1]
        if date >= month[i] and date < month[i+1]:
            file_lst[i].write(pre_combined)
            file_lst[i].write(post_combined)
            count_2019 = count_2019 + 1
        if date >= month[8]:
            count_10_2019 = count_10_2019 + 1
        



# finished scraping weather data, close all files
for file in file_lst:
    file.close()



#check if all 2019 data is recorded, True means all collected
print(len(table.find_all("tr"))-1 - count_2018 - count_10_2019 == count_2019)
# if return true, then feb to sep sport events are all collected, otherwise, something is wrong
