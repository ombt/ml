import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style
import csv
import numpy as np
import matplotlib.gridspec as gridspec
from RedisQueue import RedisQueue

f = plt.figure()

f, ax1 = plt.subplots()

f, axarr = plt.subplots(5, sharex=True)
f.subplots_adjust(hspace=0)

labels = ['Neutral','Happy','Sad','Surprise','Angry']

xv = []
yNeutral = []
yHappy = []
ySad = []
ySurprise = []
yAngry = []

#plt.style.use('fivethirtyeight')

q = RedisQueue('test')

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

    axarr[0].clear()
    axarr[1].clear()
    axarr[2].clear()
    axarr[3].clear()
    axarr[4].clear()
    
    # Neutral
    axarr[0].fill_between(xv,yNeutral, color='skyblue', alpha=0.8)
    axarr[0].plot(xv, yNeutral, color='skyblue', lw=2)

    # Happy
    axarr[1].fill_between(xv,yHappy, color='green', alpha=0.8)
    axarr[1].plot(xv, yHappy, color='green', lw=2)

    # Sad
    axarr[2].fill_between(xv,ySad, color='grey', alpha=0.8)
    axarr[2].plot(xv, ySad, color='grey', lw=2)

    # Surprise
    axarr[3].fill_between(xv,ySurprise, color='yellow', alpha=0.8)
    axarr[3].plot(xv, ySurprise, color='yellow', lw=2)

    # Angry
    axarr[4].fill_between(xv,yAngry, color='red', alpha=0.8)
    axarr[4].plot(xv, yAngry, color='red', lw=2)

    if len(xv) > 1:
        axarr[0].xaxis.set_ticks(np.arange(min(xv), max(xv)+1, 10))

    axarr[0].set_yticks([])
    axarr[1].set_yticks([])
    axarr[2].set_yticks([])
    axarr[3].set_yticks([])
    axarr[4].set_yticks([])
    
    axarr[0].set_title('Emotion Recognition Trend over Time')
    axarr[0].set_ylabel("Neutral")
    axarr[1].set_ylabel("Happy")
    axarr[2].set_ylabel("Sad")
    axarr[3].set_ylabel("Surprise")
    axarr[4].set_ylabel("Angry")

    axarr[0].grid(True)
    axarr[1].grid(True)
    axarr[2].grid(True)
    axarr[3].grid(True)
    axarr[4].grid(True)
    
    axarr[5].clear()
    
    ySentiment = [yN, yH, ySa, ySu, yA]

    colors = ['lightskyblue', 'yellowgreen', 'grey', 'gold', 'lightcoral']
    #ax1.pie(ySentiment, colors=colors, labels=labels, autopct=my_autopct, shadow=True, startangle=90)
    axarr[5].pie(ySentiment, colors=colors, labels=my_level_list(ySentiment), autopct=my_autopct, shadow=True, startangle=90)
    axarr[5].axis('equal')
    plt.tight_layout()
    
    centre_circle = plt.Circle((0,0),0.70,fc='white')
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)

ani1 = animation.FuncAnimation(f, animate, interval=1000)
plt.show()
