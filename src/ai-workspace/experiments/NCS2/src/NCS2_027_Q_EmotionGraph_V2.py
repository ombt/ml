import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style
import csv
import numpy as np
from RedisQueue import RedisQueue


f, axarr = plt.subplots(5, sharex=True)
f.subplots_adjust(hspace=0)

xv = []
yNeutral = []
yHappy = []
ySad = []
ySurprise = []
yAngry = []

#plt.style.use('fivethirtyeight')

q = RedisQueue('test')

def animate(i):  
    global xv
    global yNeutral
    global yHappy
    global ySad
    global ySurprise
    global yAngry
        
    while q.empty() is False:
        msgBody = q.get()
        print("Result is <{}>".format(msgBody))
        row = str(msgBody).split(',')
        
        xVal = int(row[2])
        yN = int(row[4])
        yH = int(row[5])
        ySa = int(row[6])
        ySu = int(row[7])
        yA = int(row[8][:-1])
    
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
        yNeutral.append(yN)
        yHappy.append(yH)
        ySad.append(ySa)
        ySurprise.append(ySu)
        yAngry.append(yA)
    
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
    
    
ani1 = animation.FuncAnimation(f, animate, interval=1000)
plt.show()
