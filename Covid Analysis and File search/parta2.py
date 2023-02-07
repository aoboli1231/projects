import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sys

# get the name of the output file
# collect image name from command
img_a=sys.argv[1]
img_b=sys.argv[2]

print("a")

# line 11 - 66 is copied from parta1, but corrected into yearly bases
# read csv file
df = pd.read_csv('https://covid.ourworldindata.org/data/owid-covid-data.csv',encoding = 'ISO-8859-1')

# select the data that needs to be processed, seperately, it's easier to
# process data seperately then form together
data = df.loc[:,['location','date','new_cases','new_deaths']]
total_case=df.loc[:,['location','date','total_cases','total_deaths']]


#convert the date into yearly period, into extra column
total_case['date'] = pd.to_datetime(total_case['date'])
total_case['year'] = total_case['date'].dt.to_period('Y')
data['date'] = pd.to_datetime(data['date'])
data['year'] = data['date'].dt.to_period('Y')


# select only 2020 year data
year2020 = data['date'].map(lambda x : x.year == 2020)

# obtained monthly record, can delete column containing dates
sorted_data = data[year2020]
total = total_case[year2020]
del sorted_data['date']
del total['date']


# monthly new cases are sumed, monthly total death are the maximum
# group both dataframe into location and month
sorted_new = sorted_data.groupby(['location','year']).sum()
sorted_total = total.groupby(['location','year']).max()

#put two data together
covid_2020 = pd.concat([sorted_new, sorted_total], axis=1)

# set total deaths and total cases as a variable
total_deaths = covid_2020['total_deaths']
total_cases = covid_2020['total_cases']
# calculate series of case fatality rate, name the series
case_fatality_rate = total_deaths/total_cases
case_fatality_rate.name = "case_fatality_rate"


case_fatality_rate = pd.DataFrame({"case_fatality_rate":case_fatality_rate})

# combine covid_2020 and the case fatality rate, order the columns
covid_2020 = pd.concat([case_fatality_rate,covid_2020], axis=1)
covid_2020 = covid_2020[["case_fatality_rate","total_cases",
                           "new_cases","total_deaths","new_deaths"]]

# reset the index, then set the index as location again
covid_2020.reset_index(inplace=True)
covid_2020 = covid_2020.set_index("location")
case_fatality_rate = covid_2020["case_fatality_rate"]
new_cases =  covid_2020["new_cases"]



# save the scatter plot as img_a from command, name the x and y
# axis, as well as the title
plt.scatter(new_cases,case_fatality_rate,c=case_fatality_rate)
plt.xlabel("new cases")
plt.ylabel("case fatality rate")
plt.title("case fatality rate vs. new cases for all countries")
cbr = plt.colorbar()
cbr.set_label('case fatality rate')
plt.grid(True)
plt.savefig(img_a)

# save the scatter plot as img_b from command, name the x and y
# axis, as well as the title, including the scale
plt.scatter(new_cases,case_fatality_rate,c=case_fatality_rate)
plt.xscale('log')
plt.xlabel("new cases")
plt.ylabel("case_fatality_rate")
plt.title("case_fatality_rate vs. new_cases for all countries (log scale)")
cbr.set_label('case fatality rate')
plt.grid(True)
plt.savefig(img_b)


