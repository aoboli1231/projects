import pandas as pd
from datetime import datetime, timedelta
from dateutil import parser
import os
import pathlib
# now filter the sport event data to event time intervals
# change folder's working directory
print("Enter Project 1 folder Working directory(with no backslash at the end):")
directory = input()

pathlib.Path(directory + "data/raw/sport/parquet/").mkdir(parents=True, exist_ok=True)


# define a function that sorts sport's csv data, remove duplicate time intervals, and write out time intervals if an event happens. E.g. 12:00-13:00 baseball entrance time, 12:30-13:30 basketball entrance time, time interval 12:00-13:30 will have a sports event happening
def sort_sport(file_s, file_write):
    # create a list to obtain the starting and ending date
    dates = []
    # read sport event start time and end time, this includes hh:mm:ss
    start_day = list(file_s.Date)
    start_time_s = list(file_s.start_time)
    end_time_s = list(file_s.end_time)
    
    # create two empty list to append time intervals
    sport_event_start = []
    sport_event_end = []
    
    # check if start_time_s matches that day's data
    for i in range(len(start_time_s)):
        # 0:10 of the sport start time is date
        date = start_day[i][:10]
        start_t_s = start_time_s[i]
        end_t_s = end_time_s[i]
        # if date in dates, then it means sport event's interval for the day is not all sorted
        if date not in dates:
            # append date in dates
            dates.append(date)
            # write up the current event time interval
            for k in range(len(sport_event_start)):
                combined = str(sport_event_start[k])+","+str(sport_event_end[k])+",1"+"\n"
                file_write.write(combined)
            # empty the time interval
            sport_event_start = []
            sport_event_end = []
        
        # now that day is in dates, compute time intervals
        # check if list is empty first
        if len(sport_event_start) == 0:
            # this means that the sport_event_start is empty, add the first time interval
            sport_event_start.append(start_t_s)
            sport_event_end.append(end_t_s)
            continue
        
    
        # compare the events time interval
        # if endtime is before the a start time, it's another interval, add another interval
        check = 0
        for k in range(len(sport_event_start)):
        
            # if start time is before any start time, and end time is also before any start time, it's unique hour
            if start_t_s < sport_event_start[k] and end_t_s < sport_event_start[k]:
                continue
                
            # if end time is after any end time,and start time is after any end time, it's new time interval
            if start_t_s > sport_event_end[k] and end_t_s > sport_event_end[k]:
                continue
            
            # time is found in list, update new time interval
            else:
                check = 1
                break
                
        # when check is 0, it's new time interval, add to start and end time
        if check == 0:
            sport_event_start.append(start_t_s)
            sport_event_end.append(end_t_s)
        
        else:
            # update current time interval
            for k in range(len(sport_event_start)):
                # if start time is before any start time, and end time is also before any start time, it's unique hour
                if start_t_s < sport_event_start[k] and end_t_s >= sport_event_start[k] and end_t_s <= sport_event_end[k]:
                    # undate start time interval
                    sport_event_start[k] = start_t_s
                    break
                if start_t_s < sport_event_start[k] and end_t_s >= sport_event_start[k] and end_t_s >= sport_event_end[k]:
                    # undate start time interval
                    sport_event_start[k] = start_t_s
                    sport_event_end[k] = end_t_s
                    break
                if start_t_s > sport_event_start[k] and start_t_s <= sport_event_end[k] and end_t_s > sport_event_end[k]:
                    # undate start time interval
                    sport_event_end[k] = end_t_s
                    break

    # write all list out
    for j in range(len(sport_event_start)):
        combined = str(sport_event_start[k])+","+str(sport_event_end[k])+",1"+"\n"
        file_write.write(combined)
    





# go through all raw csv files in the folder, and convert time intervals
for filename in os.listdir(directory+"/data/raw/sport/csv"):
    # file_m_s represent file_month_sort
    if filename[0:5] =="sport":
        file_m_s = pd.read_csv(directory+"/data/raw/sport/csv/"+filename)
    else:
        continue
    # file_m represent file month, scraped csv for each month, create this new file
    # to change raw time intervals
    file_m = open(directory+"/data/raw/sport/csv/final_"+filename, 'w')
    file_m.write("start_time"+","+"end_time"+","+"event"+"\n")
    sort_sport(file_m_s, file_m)
    file_m.close()


# now only read final_csv, and convert to parquet
for filename in os.listdir(directory+"/data/raw/sport/csv"):
    # file_m_s represent file_month_sort
    if filename[0:5] =="final":
        df = pd.read_csv(directory+"/data/raw/sport/csv/"+filename)
    else:
        continue
    # convert all event column to numeric value, for future analysis
    df['event'] = pd.to_numeric(df['event'])
    # save new dataframe as parquet to obtain all column datatype
    df.to_parquet(directory+"/data/raw/sport/parquet/"+filename[:-3]+"parquet")
    





