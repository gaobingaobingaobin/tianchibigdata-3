# -*- coding: utf-8 -*-
import numpy as np
from matplotlib import pyplot as plt
import os
import json
import MySQLdb

import itertools

               
                                                                                    
if __name__=="__main__":
    
    try:
        db = MySQLdb.connect("localhost","root","uiop.123.123","tianchi_bigdata" )
        cursor=db.cursor()
    except Exception as error:
        print 'connect eroor'+str(error)
    orginal_songs_artist_map_sql='select song_id,artist_id from mars_tianchi_songs order by artist_id'
    orginal_user_actions='select user_id,song_id,action_type,Ds from mars_tianchi_user_actions order by song_id'
    try:
        cursor.execute(orginal_songs_artist_map_sql)
        mars_tianchi_songs=cursor.fetchall()
        cursor.execute(orginal_user_actions)
        mars_tianchi_user_actions=cursor.fetchall()
    except Exception as error:
            print 'connect eroor'+str(error)
    print "ok in get data"
    '''artist_song_map={key:map(lambda x:x[0],value)  for key,value in itertools.groupby(mars_tianchi_songs,key=lambda x:x[-1])}
    song_user_actions={key:list(value) for key,value in itertools.groupby(mars_tianchi_user_actions,key=lambda x:x[1])}'''
    sun={key:len(list(value))for key,value in itertools.groupby(sorted(mars_tianchi_user_actions,key=lambda item:int(item[-1])),key=lambda x:int(x[-1]))}
    sun_1={key:len(list(value))for key,value in itertools.groupby(sorted(filter(lambda x:x[-2]=='1',mars_tianchi_user_actions),key=lambda item:int(item[-1])),key=lambda x:int(x[-1]))}
    sun_2={key:len(list(value))for key,value in itertools.groupby(sorted(filter(lambda x:x[-2]=='2',mars_tianchi_user_actions),key=lambda item:int(item[-1])),key=lambda x:int(x[-1]))}
    sun_3={key:len(list(value))for key,value in itertools.groupby(sorted(filter(lambda x:x[-2]=='3',mars_tianchi_user_actions),key=lambda item:int(item[-1])),key=lambda x:int(x[-1]))}
    color=['r','g','y','o']
    col=0
    y_all_list= [[datalist[item] for item in sorted(datalist.keys()) ] for datalist in [sun,sun_1,sun_2,sun_3]]
    max_play=max(y_all_list[0])
    y_all_list_one=[[float(play)/float(max_play) for play in item] for item in y_all_list]
    
    for ylist in y_all_list_one:
        plt.plot(ylist,color[col])
        col+=1
    plt.show()