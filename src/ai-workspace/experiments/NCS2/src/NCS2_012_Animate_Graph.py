import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style
import csv
import numpy as np

fig = plt.figure()
ax3 = fig.add_subplot(2,4,1)
ax3.set_xticks([])
ax3.set_yticks([])
ax1 = fig.add_subplot(2,4,(2,4))
ax4 = fig.add_subplot(2,4,5)
ax4.set_xticks([])
ax4.set_yticks([])
ax2 = fig.add_subplot(2,4,(6,8))

#plt.style.use('fivethirtyeight')


def animate(i):
    # csvfile = open('/home/s/ai-workspace/experiments/NCS2/data/object_data_file.csv','r')
    csvfile = open('/root/sandbox/ml/src/ai-workspace/experiments/NCS2/data/object_data_file.csv','r')
    plots = csv.reader(csvfile, delimiter=',')
    xp = []
    yp = []
    xv = []
    yv = []
    pObjCtr = 0
    vObjCtr = 0

    yprops = dict(rotation=90,
              horizontalalignment='right',
              verticalalignment='center',
              x=0)
    
    for row in plots:
        if row[0] == "Vehicles" :       

            if len(xv) > 50:
                 del  xv[0]
                 del  yv[0]
            xv.append(int(row[2]))
            yv.append(int(row[3]))
            vObjCtr = int(row[4])
        else:
            if len(xp) > 50:
                 del  xp[0]
                 del  yp[0]

            xp.append(int(row[2]))
            yp.append(int(row[3]))
            pObjCtr = int(row[4])
    ax1.clear()
    ax2.clear()
    ax3.clear()
    ax4.clear()
    ax1.grid(True)
    ax2.grid(True)

    ax3.grid(False)
    ax4.grid(False)
    ax3.set_xticks([])
    ax3.set_yticks([])
    ax4.set_xticks([])
    ax4.set_yticks([])
    

    ax1.fill_between(xp,yp, color='lightgreen', alpha=0.4)
    ax1.fill_between(xp,0, yp, where=(7 < np.array(yp)), color='red', alpha=0.4)

    #ax1.set_title( 'Persons' )
    # ax1.set_ylabel('Persons', **yprops)
    ax1.plot(xp, yp)

    ax2.fill_between(xv,yv, color='skyblue', alpha=0.4)
    ax2.fill_between(xv,0, yv, where=(6 < np.array(yv)), color='red', alpha=0.4)

    #ax2.set_title('Vehicles')
    # ax2.set_ylabel('Vehicles', **yprops)    
    ax2.plot(xv, yv)

    ax3.set_title('People')
    ax3.text(0.5, 0.5, 
          '{}'.format(pObjCtr), 
          ha='center', va='center',
          fontsize=25, 
          color="g")
    ax3.axis('Off')
    
    ax4.set_title('Vehicles')
    ax4.text(0.5, 0.5, 
          '{}'.format(vObjCtr), 
          ha='center', va='center',
          fontsize=25, 
          color="b")
    ax4.axis('Off')
    
    
ani1 = animation.FuncAnimation(fig, animate, interval=1000)
plt.show()
