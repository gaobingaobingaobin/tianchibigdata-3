# -*- coding: utf-8 -*-
import json
from matplotlib import pyplot as plt

with open("data.json","rt") as f:
    songs_artist=json.load(f)
i=0
colors=['r-','b-','g-','y-']
for key,values in songs_artist.items():
    plt.plot(values,colors[i],label="artist_id"+key)
    i+=1

plt.xlabel("0300--0825")
plt.ylabel("plays time a day")
plt.savefig("four.png")
    
    