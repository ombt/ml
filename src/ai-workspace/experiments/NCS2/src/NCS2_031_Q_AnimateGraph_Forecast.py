import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style
import csv
import numpy as np
# AR example
from statsmodels.tsa.ar_model import AR

fig = plt.figure()
ax3 = fig.add_subplot(2,4,1)
ax3.set_xticks([])
ax3.set_yticks([])
ax1 = fig.add_subplot(2,4,(2,4))
ax4 = fig.add_subplot(2,4,5)
ax4.set_xticks([])
ax4.set_yticks([])
ax2 = fig.add_subplot(2,4,(6,8))

xp_forecast = []
yp_forecast = []
xv_forecast = []
yv_forecast = []

def animate(i):
    
    #global xp_forecast, yp_forecast, xv_forecast, yv_forecast
    
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

        xVal = int(row[2])
        yVal = int(row[3])
         
        if row[0] == "Vehicles" :
            if xVal == 0:
                xv.clear()
                yv.clear()
                xv_forecast.clear()
                yv_forecast.clear()
            if len(xv) > 50:
                del  xv[0]
                del  yv[0]
            xv.append(xVal)
            yv.append(yVal)
            vObjCtr = int(row[4])
        else:
            if row[0] == "Persons" :
                if xVal == 0:
                    xp.clear()
                    yp.clear()
                if len(xp) > 50:
                    del  xp[0]
                    del  yp[0]
                xp.append(xVal)
                yp.append(yVal)
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
    
    # fit model
    if len(yp) > 50:
        model = AR(yp)
        model_fit = model.fit()
        
        # make prediction
        yhat = model_fit.predict(len(yp), len(yp))
        
        if not xp_forecast:
            print("Persons - List is empty {}".format(xp_forecast))
            xp_forecast.append(xp[len(xp)-1]+2)
            yp_forecast.append(yhat)
        else:
            try:
                xp_forecast_val = xp_forecast[len(xp_forecast)-1]
                xp_val = xp[len(xp)-1]+2
                print("Persons - Comparing {} {}".format(xp_forecast_val, xp_val))

                if xp_forecast_val > xp_val:
                    xp_forecast.clear()
                    yp_forecast.clear()
                else :
                    if xp_forecast_val+2 - xp_forecast[0] > 50:
                        print("Person Deleting {} {}".format(xp_forecast[0],yp_forecast[0]))
                        del  xp_forecast[0]
                        del  yp_forecast[0]

                if xp_forecast_val != xp_val : 
                    xp_forecast.append(xp_val)
                    yp_forecast.append(yhat)
            
            except ValueError:
                print("Ignore the exception")
            
        print("Person - Predicted next value is {}, {}, len - {}".format(xp_forecast[len(xp_forecast)-1], 
                                                                         yhat, len(xp_forecast)))

        ax1.plot(xp_forecast, yp_forecast, 'r.', markersize=12)


    ax2.fill_between(xv,yv, color='skyblue', alpha=0.4)
    ax2.fill_between(xv,0, yv, where=(6 < np.array(yv)), color='red', alpha=0.4)

    #ax2.set_title('Vehicles')
    # ax2.set_ylabel('Vehicles', **yprops)    
    ax2.plot(xv, yv)

    # fit model
    if len(yv) > 50:
        model = AR(yv)
        model_fit = model.fit()
        
        # make prediction
        yhat = model_fit.predict(len(yv), len(yv))


        if not xv_forecast:
            print("Vehicles - List is empty {}".format(xv_forecast))
            xv_forecast.append(xv[len(xv)-1]+2)
            yv_forecast.append(yhat)
        else:
            try:
                xv_forecast_val = xv_forecast[len(xv_forecast)-1]
                xv_val = xv[len(xv)-1]+2
                print("Vehicles - Comparing {} {}".format(xv_forecast_val, xv_val))

                if xv_forecast_val > xv_val:
                    xv_forecast.clear()
                    yv_forecast.clear()
                else :
                    if xv_forecast_val+2 - xv_forecast[0] > 50:
                        print("Vehicles Deleting {} {}".format(xv_forecast[0],yv_forecast[0]))
                        del  xv_forecast[0]
                        del  yv_forecast[0]

                if xv_forecast_val != xv_val : 
                    xv_forecast.append(xv_val)
                    yv_forecast.append(yhat)
            
            except ValueError:
                print("Ignore the exception")
            
        print("Vehicles - Predicted next value is {}, {}, len - {}".format(xv_forecast[len(xv_forecast)-1], 
                                                                           yhat, len(xv_forecast)))
        ax2.plot(xv_forecast, yv_forecast, 'r.', markersize=12)


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





'''
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from RedisQueue import RedisQueue
import pandas as pd
import numpy as np
import csv

fig = plt.figure()
ax3 = fig.add_subplot(2,4,1)
ax3.set_xticks([])
ax3.set_yticks([])
ax1 = fig.add_subplot(2,4,(2,4))
ax4 = fig.add_subplot(2,4,5)
ax4.set_xticks([])
ax4.set_yticks([])
ax2 = fig.add_subplot(2,4,(6,8))
xp = []
yp = []
xv = []
yv = []
pObjCtr = 0
vObjCtr = 0


#plt.style.use('fivethirtyeight')

q = RedisQueue('test')

def animate(i):  
    global xp
    global yp
    global xv
    global yv
    global pObjCtr 
    global vObjCtr 
    
    csvfile = open('/home/s/ai-workspace/experiments/NCS2/data/object_data_file.csv','r')
    plots = csv.reader(csvfile, delimiter=',')
    
    yprops = dict(rotation=90,
              horizontalalignment='right',
              verticalalignment='center',
              x=0)
    
    for row in plots:
    
        xVal = int(row[2])
        yVal = int(row[3])
        if row[0] == "Vehicles" :       
            if len(xv) > 50:
                if xv[0] > xVal:
                    xv.clear()
                    yv.clear()
                else:
                    del  xv[0]
                    del  yv[0]
            xv.append(xVal)
            yv.append(yVal)
            vObjCtr = int(row[4])
        else:
            if row[0] == "Persons" :
                if len(xp) > 50:
                    if xp[0] > xVal:
                        xp.clear()
                        yp.clear()
                    else:
                        del  xp[0]
                        del  yp[0]

                xp.append(xVal)
                yp.append(yVal)
                pObjCtr = int(row[4])
    ax1.clear()
    #ax_forecast.clear()
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

    # fit model
    if len(yp) > 30:
        model = AR(yp)
        model_fit = model.fit()
        
        # make prediction
        yhat = model_fit.predict(yp, len(yp))
        print("Predicted next value is {}".format(yhat))

        xp_forecast = [xp[len(xp)-1]+2]
        yp_forecast = [yhat]
        ax_forecast.plot(xp_forecast, yp_forecast, 'r.')
        ax_forecast.tick_params('y', colors='r')

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
'''
