import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style
import csv
import numpy as np
from RedisQueue import RedisQueue

f, ax1 = plt.subplots()
labels = ['Neutral','Happy','Sad','Surprise','Angry']

ySentiment = []

q = RedisQueue('test')

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
        
    yTotal = yN+yH+ySa+ySu+yA
    
    if yTotal > 0:    
        yN = yN/yTotal
        yH = yH/yTotal
        ySa = ySa/yTotal
        ySu = ySu/yTotal
        yA = yA/yTotal    

    ax1.clear()
    
    ySentiment = [yN, yH, ySa, ySu, yA]
    
    colors = ['lightskyblue', 'yellowgreen', 'grey', 'gold', 'lightcoral']
    #ax1.pie(ySentiment, colors=colors, labels=labels, autopct=my_autopct, shadow=True, startangle=90)
    ax1.pie(ySentiment, colors=colors, labels=my_level_list(ySentiment), autopct=my_autopct, shadow=True, startangle=90)
    ax1.axis('equal')
    plt.tight_layout()
    
    centre_circle = plt.Circle((0,0),0.70,fc='white')
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)
        
ani1 = animation.FuncAnimation(f, animate, interval=1000)
plt.show()
