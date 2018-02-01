import matplotlib.pyplot as plt
import numpy as np
import string
import collections
import csv

""" Load File """

with open('output_01_02_2018_v1.csv', 'rb') as csvfile:
    total_data = []
    data = csv.reader(csvfile)
    for row in data:
        total_data.append(row)

""" Plot Data """

color = ['-ob','-og','-ok','-oy']
names = []
c = 0
for i,item in enumerate(total_data):
    #if c < 20:
    a = item[0]
    b = a.split(', [')
    name = b[0][1:]
    if name in ("u'war'","u'labor'","u'foreign'","u'freedom"):
        data = []
        names.append(name)
        a = b[1].split('), (')
        for algo in a:
            for elemento in ('u())],"'):
                algo = algo.replace(elemento,"")
            values = algo.split(' ')
            values = (values[0][1:-1], values[1])
            data.append(values)
        plt = reorder_plot(data,name,color[c],plt)
        c+=1

ax = plt.subplot(111)
# Hide the right and top spines
ax.spines['right'].set_visible(False)
ax.spines['top'].set_visible(False)
# Only show ticks on the left and bottom spines
ax.yaxis.set_ticks_position('left')
ax.xaxis.set_ticks_position('bottom')
plt.xlabel('Year')
plt.ylabel('Occurrence Ratio')
plt.legend(names,shadow=True)
plt.ylim(ymin=.5,ymax=3)
plt.xlim(xmin=1900,xmax=1950)
plt.tick_params(top='off', right='off')
plt.show()

""" Plot Function """

def reorder_plot(data,name,color,plt):
    dc = dict()
    # Get Data Together
    for item in data:
        if item[0] not in dc.keys():
            dc[item[0]] = float(item[1])
        else:
            dc[item[0]] = (dc[item[0]][0]+int(item[1]),dc[item[0]][1]+int(item[2]))
    od_dc = collections.OrderedDict(sorted(dc.items()))
    # Re-order to plot
    year = np.empty([len(od_dc), 1])
    occu_ratio = np.empty([len(od_dc), 1])
    for i,key in enumerate(od_dc.keys()):
        year[i,0] = int(key)
        occu_ratio[i,0] = float(od_dc[key])
    if name == "u'war'":
        plt.plot(year,occu_ratio,'-or',linewidth=3.0)
    else:
        plt.plot(year,occu_ratio,color)
    return plt
