assignment_2

import pandas as pd
import matplotlib.pyplot as plt

# Read the excel sheet from local direction to pandas dataframe
tree_loss = pd.read_excel("tree_coverage_loss.xlsx")
cancer_cases = pd.read_excel("cancer_cases.xlsx")

t_loss = []
c_cases = []
# convert list to seris, then to dataframe, set dataframe index
t_loss = pd.Series(tree_loss)
c_cases = pd.Series(cancer_cases)
cases_vs_loss = pd.DataFrame({'tree coverage loss':t_loss, 'cancer cases':c_cases})
cases_vs_loss.set_index()

# set x and y index
x=cases_vs_loss['tree coverage loss']
y=cases_vs_loss['cancer cases']


# draw the data on the same graph into scatter plot
plt.scatter(x, y, colors='blue')
plt.title('cancer cases vs tree coverage loss')
# set axis labels
plt.xlabel('TREE COVERAGE LOSS (kha)')
plt.ylabel('CANCER CASES PER 10K')
# save this figure in the same direction
plt.savefig("CANCER CASES.png")
