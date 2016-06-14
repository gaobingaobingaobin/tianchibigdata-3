# -*- coding: utf-8 -*-
import numpy as np
from matplotlib import pyplot as plt
import MySQLdb
import itertools

sql_songs_play="select user_id,Ds from mars_tianchi_user_actions  where action_type=1 and song_id={id} order by Ds" 
 
sql_songs_allaction="selct Ds from mars_tinachi_user_actions where song_id={songid}"

if __name__ =="__main__":
    try:
        db = MySQLdb.connect("localhost","root","uiop.123.123","tianchi_bigdata" )
        print "succed to connect to local database"
    except Exception as e:
        print "got a error"+str(e)
        
    cursor = db.cursor()
    
    song_id_list=["12","30","50","70","80"]
    
    try:
        
        cursor.execute(sql_songs_play.format(id=31))
        infor=cursor.fetchall()
        print len(infor)
        play_counts=sorted([(int(k),len(list(g))) for k,g in itertools.groupby(infor,key=lambda x:x[-1])],key=lambda y:y[0])
        print play_counts
        xlist=[item[0]-20150000 for item in play_counts] 
        ylist=[item[-1]  for item in play_counts]
        plt.plot(xlist,ylist,"r")
        plt.show()
        
    except Exception as erorr:
        print "cursor error"+str(erorr)
   
   
    finally:
        db.close()