# -*- coding: utf-8 -*-
"""
Created on Mon Nov 28 14:08:25 2016

@author: AKumara
"""

import matplotlib.pyplot as plt
import pandas as pd

bike_ride = pd.read_csv("tripsbygeneration.csv")
df = pd.DataFrame({'generation':bike_ride['generation'],'trips':bike_ride['trips']})
ax = df.plot.bar(stacked=True, color=['c'])

#ax = plt.figure(figsize=(10,10), dpi=300).add_subplot(111)
ax.set_xticklabels(df['generation'])

plt.title('September - Citi Bike trends - Generation')
plt.ylabel('Bike Trips')
plt.xlabel('Generation')

plt.gcf().set_size_inches(10, 10)
plt.savefig("tripsbygeneration.png", dpi=300) 