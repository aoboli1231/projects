import numpy as np
import matplotlib.pyplot as plt
n_groups = 3
reading_time = (206, 205, 228)
process_time = (516, 68, 71)
total_time = (734, 291, 318)

# create plot
fig, ax = plt.subplots()  # increase the figure size
index = np.arange(n_groups)
bar_width = 0.2
opacity = 1

rects1 = plt.bar(index, reading_time, bar_width, alpha=opacity, color='steelblue', label='reading time')
rects2 = plt.bar(index + bar_width, process_time, bar_width, alpha=opacity, color='darkseagreen', label='process time')
rects3 = plt.bar(index + 2*bar_width, total_time, bar_width, alpha=opacity, color='orange', label='total time')

plt.ylabel('Time(s)')
plt.title("Performance of different node and core")
plt.xticks(index + bar_width, ('1 node 1 core', '1 node 8 core', '2 node 8 core'))
plt.legend()

# Add bar values on top of the bars
for i, rect in enumerate(rects1 + rects2 + rects3):
    height = rect.get_height()
    ax.text(rect.get_x() + rect.get_width() / 2., 1.05 * height,
            '%d' % int(height), ha='center', va='bottom')
    
plt.ylim(0, max(total_time) * 1.2)  # adjust the y-axis limit to accommodate the largest value

plt.tight_layout()

plt.savefig('performance evaluation.png')
plt.show()
