import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style
import csv
import numpy as np
import matplotlib.gridspec as gridspec
from RedisQueue import RedisQueue
 

fig = plt.figure(0)
ax1 = plt.subplot2grid((5,3), (0, 0), rowspan=5)
ax2 = plt.subplot2grid((5,3), (0, 1), colspan=2)
ax3 = plt.subplot2grid((5,3), (1, 1), colspan=2)
ax4 = plt.subplot2grid((5,3), (2, 1), colspan=2)
ax5 = plt.subplot2grid((5,3), (3, 1), colspan=2)
ax6 = plt.subplot2grid((5,3), (4, 1), colspan=2)
fig.subplots_adjust(hspace=0)

xv = []
yNeutral = []
yHappy = []
ySad = []
ySurprise = []
yAngry = []

#plt.style.use('fivethirtyeight')

q = RedisQueue('test')

labels = ['Neutral','Happy','Sad','Surprise','Angry']
ySentiment = []

def my_autopct(pct):
    return ('%i%%' % pct) if pct > 0 else ''

def my_level_list(data):
    list = []
    for i in range(len(data)):
        if data[i] > 0 : #2%
            list.append(labels[i])
        else:
            list.append('')
    return list


def animate(i):  
    global xv
    global yNeutral
    global yHappy
    global ySad
    global ySurprise
    global yAngry
    global ySentiment
    
    yN = 0.0
    yH = 0.0
    ySa = 0.0
    ySu = 0.0
    yA = 0.0
      
    while q.empty() is False:
        msgBody = q.get()
        print("Result is <{}>".format(msgBody))
        row = str(msgBody).split(',')
        
        yN += float(row[4])
        yH += float(row[5])
        ySa += float(row[6])
        ySu += float(row[7])
        yA += float(row[8][:-1])
           
        xVal = int(row[2])
            
        if len(xv) > 0 and xv[0] > xVal:
            xv.clear()
            yNeutral.clear()
            yHappy.clear()
            ySad.clear()
            ySurprise.clear()
            yAngry.clear()
        else:
            if len(xv) > 50:
                del  xv[0]
                del  yNeutral[0]
                del  yHappy[0]
                del  ySad[0]
                del  ySurprise[0]
                del  yAngry[0]
        xv.append(xVal)

        yNeutral.append(int(row[4]))
        yHappy.append(int(row[5]))
        ySad.append(int(row[6]))
        ySurprise.append(int(row[7]))
        yAngry.append(int(row[8][:-1]))
    
    yTotal = yN+yH+ySa+ySu+yA
    
    if yTotal > 0:    
        yN = yN/yTotal
        yH = yH/yTotal
        ySa = ySa/yTotal
        ySu = ySu/yTotal
        yA = yA/yTotal    
    
    ax2.clear()
    ax3.clear()
    ax4.clear()
    ax5.clear()
    ax6.clear()
    
    # Neutral
    ax2.fill_between(xv,yNeutral, color='lightskyblue', alpha=0.8)
    ax2.plot(xv, yNeutral, color='lightskyblue', lw=2)

    # Happy
    ax3.fill_between(xv,yHappy, color='yellowgreen', alpha=0.8)
    ax3.plot(xv, yHappy, color='yellowgreen', lw=2)

    # Sad
    ax4.fill_between(xv,ySad, color='grey', alpha=0.8)
    ax4.plot(xv, ySad, color='grey', lw=2)

    # Surprise
    ax5.fill_between(xv,ySurprise, color='gold', alpha=0.8)
    ax5.plot(xv, ySurprise, color='gold', lw=2)

    # Angry
    ax6.fill_between(xv,yAngry, color='lightcoral', alpha=0.8)
    ax6.plot(xv, yAngry, color='lightcoral', lw=2)

    if len(xv) > 1:
        ax2.xaxis.set_ticks(np.arange(min(xv), max(xv)+1, 10))

    ax2.set_yticks([])
    ax3.set_yticks([])
    ax4.set_yticks([])
    ax5.set_yticks([])
    ax6.set_yticks([])
    
    ax2.set_xticks([])
    ax3.set_xticks([])
    ax4.set_xticks([])
    ax5.set_xticks([])
    
    ax2.set_title('Emotion Recognition Trend over Time')
    ax2.set_ylabel("Neutral")
    ax3.set_ylabel("Happy")
    ax4.set_ylabel("Sad")
    ax5.set_ylabel("Surprise")
    ax6.set_ylabel("Angry")

    ax2.yaxis.set_label_position("right")
    ax3.yaxis.set_label_position("right")
    ax4.yaxis.set_label_position("right")
    ax5.yaxis.set_label_position("right")
    ax6.yaxis.set_label_position("right")

    ax2.grid(True)
    ax3.grid(True)
    ax4.grid(True)
    ax5.grid(True)
    ax6.grid(True)
    
    ax2.set_frame_on(False)
    ax3.set_frame_on(False)
    ax4.set_frame_on(False)
    ax5.set_frame_on(False)
    ax6.set_frame_on(False)
    
    ax1.clear()
    
    ySentiment = [yN, yH, ySa, ySu, yA]

    colors = ['lightskyblue', 'yellowgreen', 'grey', 'gold', 'lightcoral']
    #ax1.pie(ySentiment, colors=colors, labels=labels, autopct=my_autopct, shadow=True, startangle=90)
    ax1.pie(ySentiment, colors=colors, labels=my_level_list(ySentiment), autopct=my_autopct, startangle=90)
    ax1.axis('equal')
    plt.tight_layout()
    
    centre_circle = plt.Circle((0,0),0.75,fc='white')
    ax1.add_artist(centre_circle)
    fig.subplots_adjust(hspace=0)

ani1 = animation.FuncAnimation(fig, animate, interval=500)
plt.show()

#plt.suptitle("subplot2grid")
#make_ticklabels_invisible(plt.gcf())
#plt.show()

