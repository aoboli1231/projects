import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats


# Read the excel sheet from local direction to pandas dataframe
v_coverage = pd.read_excel("greening_area.xlsx")

suicide_rate = pd.read_excel("aihw-suicide-and-self-harm-monitoring-nmd-suicide-icd-10-x60-x84-y87-0.xlsx", sheet_name="Table NMD S11")
# take only victoria's data
suicide_rate = suicide_rate[90:156]
# take column of Suburb, 2018 and 2019 data
suicide_rate = suicide_rate.loc[:,['Unnamed: 2','Unnamed: 7']]
# rename the dataframe into proper heading
suicide_rate = suicide_rate.rename(columns = {'Unnamed: 2': 'suburb', 'Unnamed: 7': 'rate'}, inplace = False)

# take column of Suburb, 2018 and 2019 data
coverage_area = []
coverage_data = []
s_rate = []

# go through the vegetation data, since the data is smaller
for i in range(len(v_coverage)):
    # set match as 0, indicating there's no exact match of suburb
    match = 0
    # go through suicide rates
    for j in range(90,156):
        # check if there's a exact match of suburb, if so, take out suburb and data
        if v_coverage.Council[i] == suicide_rate.suburb[j]:
            match = 1
            coverage_area.append(v_coverage.Council[i])
            coverage_data.append(v_coverage.Vegetation_coverage[i])
            s_rate.append(suicide_rate.rate[j])
            break
    # if match is 0, there's no exact match, suburb in vegetation coverage may
    # be recorded in the same box in suicide rate, then check if suicide suburb contains
    # vegeration coverage suburb
    if match == 0:
        for j in range(90,156):
            if v_coverage.Council[i] in suicide_rate.suburb[j]:
                coverage_area.append(v_coverage.Council[i])
                coverage_data.append(v_coverage.Vegetation_coverage[i])
                s_rate.append(suicide_rate.rate[j])
                break
    
# convert list to seris, then to dataframe, set dataframe index as Area(suburb)
coverage_data = pd.Series(coverage_data)
coverage_area = pd.Series(coverage_area)
s_rate = pd.Series(s_rate)
coverage_v_suicide = pd.DataFrame({'Area':coverage_area, 'Vegetation coverage %':coverage_data, 'Suicide rate %':s_rate})
coverage_v_suicide.set_index("Area")


# set x and y index
x = coverage_v_suicide['Vegetation coverage %']
y = coverage_v_suicide['Suicide rate %']


# draw relative relation of suicied rate and Vegetation
z = np.polyfit(x, y, 1)
yp = np.polyval(z, x)
plt.plot(x, yp)
plt.grid(True)

# draw the data on the same graph into scatter plot
plt.scatter(x,y)



# set axis labels, also write the relative equation
plt.ylabel("Suicide rate in every 100,000 2018(%)")
plt.xlabel("Vegetation coverage 2018(%)")
plt.title("Suicide rate 2018 vs. Vegetation coverage 2018(Victoria) \n\n y=%.6fx+%.6f"%(z[0],z[1]))
# save this figure in the same direction
plt.savefig("Coverage-suicide.png")
