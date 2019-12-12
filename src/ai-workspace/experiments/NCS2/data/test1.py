import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style
import csv
import numpy as np

fig = plt.figure()
ax1 = fig.add_subplot(2,1,1)
ax2 = fig.add_subplot(2,1,2)

def animate(i):
    csvfile = open('object_data_file.csv','r')
    plots = csv.reader(csvfile, delimiter=',')
    xp = []
    yp = []
    xv = []
    yv = []

    yprops = dict(rotation=90,
              horizontalalignment='right',
              verticalalignment='center',
              x=0)
    
    redFlagV = False
    redFlagP = False

    for row in plots:
        if row[0] == "Vehicles" :       

            if int(row[3]) < 6:
               redFlagV = False
            else:
                redFlagV = True

            if len(xv) > 50:
                 del  xv[0]
                 del  yv[0]

            xv.append(int(row[2]))
            yv.append(int(row[3]))
        else:
            if int(row[3]) < 4:
               redFlagP = False
            else:
                redFlagP = True

            if len(xp) > 50:
                 del  xp[0]
                 del  yp[0]

            xp.append(int(row[2]))
            yp.append(int(row[3]))
        
    ax1.clear()
    ax2.clear()
    ax1.grid(True)
    ax2.grid(True)

    tVal = 4
    ax1.fill_between(xp,yp, color='lightgreen', alpha=0.4)
    ax1.fill_between(xp,0, yp, where=(4 < np.array(yp)), color='red', alpha=0.4)

    ax1.set_ylabel('Persons', **yprops)
    ax1.plot(xp, yp)

    ax2.fill_between(xv,yv, color='skyblue', alpha=0.4)
    ax2.fill_between(xv,0, yv, where=(6 < np.array(yv)), color='red', alpha=0.4)

    ax2.set_ylabel('Vehicles', **yprops)    
    ax2.plot(xv, yv)
    # ax2.plot(xv, yv, 'ro')


ani1 = animation.FuncAnimation(fig, animate, interval=1000)
plt.show()
