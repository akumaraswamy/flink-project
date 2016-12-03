# -*- coding: utf-8 -*-
"""
Created on Mon Nov 28 12:53:14 2016

@author: AKumara
"""

import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.ticker as ticker


bike_ride = pd.read_csv("tripsbygender.csv")
start = pd.to_datetime("9/1/2016")
idx = pd.date_range(start, periods= 30, freq='D')
df = pd.DataFrame({'Male':bike_ride['male'],'Female':bike_ride['female']})

df.index = idx
df_ts = df.resample('D').max()

plt.figure(figsize=(10,10), dpi=300).add_subplot(111)
ax = df_ts.plot(kind='bar', x=df_ts.index, stacked=False, color=['purple','c'])

# Make most of the ticklabels empty so the labels don't get too crowded
ticklabels = ['']*len(df_ts.index)

# Every 4th ticklable shows the month and day
ticklabels[::4] = [item.strftime('%b %d\n%Y') for item in df_ts.index[::4]]

ax.xaxis.set_major_formatter(ticker.FixedFormatter(ticklabels))
plt.gcf().autofmt_xdate()
plt.gcf().set_size_inches(15, 10)

plt.ylabel('Trip Count')
plt.xlabel('Trip Date')
plt.title('September Citi Bike Trip trends - Gender')

plt.savefig("tripsbygender.png", dpi=300) 