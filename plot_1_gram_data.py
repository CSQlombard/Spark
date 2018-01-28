import matplotlib.pyplot as plt
import numpy as np
import string
import collections
import csv

""" Load File """

with open('output_28_01_2018.csv', 'rb') as csvfile:
    total_data = []
    data = csv.reader(csvfile)
    for row in data:
        total_data.append(row)

""" Plot Data """

color = ['-or','-ob','-og','-ok','-oy']
names = []
c = 0
for i,item in enumerate(total_data):
    data = []
    if item[0] in ['peace', 'war']:
        names.append(item[0])
        a = item[1].split('), (')
        for algo in a:
            for elemento in string.punctuation:
                algo = algo.replace(elemento,"")
            values = algo.split('u')
            values = values[1:]
            data.append(values)
        plt = reorder_plot(data,color[c],plt)
        c+=1

ax = plt.subplot(111)
# Hide the right and top spines
ax.spines['right'].set_visible(False)
ax.spines['top'].set_visible(False)
# Only show ticks on the left and bottom spines
ax.yaxis.set_ticks_position('left')
ax.xaxis.set_ticks_position('bottom')
plt.xlabel('Year')
plt.ylabel('Occurence Ratio')
plt.legend(names,shadow=True)
plt.ylim(ymin=.5,ymax=3)
plt.xlim(xmin=1800,xmax=2018)
plt.tick_params(top='off', right='off')
plt.show()

""" Plot Function """

def reorder_plot(data,color,plt):
    dc = dict()
    # Get Data Together
    for item in data:
        if item[0] not in dc.keys():
            dc[item[0]] = (int(item[1]),int(item[2]))
        else:
            dc[item[0]] = (dc[item[0]][0]+int(item[1]),dc[item[0]][1]+int(item[2]))
    od_dc = collections.OrderedDict(sorted(dc.items()))
    # Re-order to plot
    year = np.empty([len(od_dc), 1])
    occu_ratio = np.empty([len(od_dc), 1])
    for i,key in enumerate(od_dc.keys()):
        year[i,0] = int(key)
        occu_ratio[i,0] = float(od_dc[key][0])/float(od_dc[key][1])
    plt.plot(year,occu_ratio,color)
    return plt
