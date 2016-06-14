# -*- coding: utf-8 -*-
import numpy as np
from matplotlib import pyplot as plt
import os
import json
import MySQLdb

import itertools

sql_artist_allsong="select song_id from mars_tianchi_songs  where artist_id={artistid}"

def show_play_byday_artist(artist_songs):
          ''' take songs of artist the output is the plays times of the player from 0300 to 0831'''
          sql_user_action_songid="select song_id,Ds from mars_tianchi_user_actions where action_type=1 and song_id={songid}"
          songs_pool=[]
          num=0
          for song in artist_songs:
              try:
                  cursor.execute(sql_user_action_songid.format(songid=song))
                  if num%30==0:       
                       songs_pool.append(cursor.fetchall())
                       
              except Exception as error:
                        print "got a error in function show_play_byday_artist"+str(error)
          songs_pool.append(cursor.fetchall())
          print "no error in sql command"
          
          songs_groupby=sorted([(int(k),len(list(g))) for k,g in itertools.groupby(sorted(itertools.chain.from_iterable(songs_pool),key=lambda z:z[-1]),key=lambda x:int(x[-1]))],key=lambda item:item[0])
          return songs_groupby

                             
                             
                
                                  
                                                                                    
if __name__=="__main__":
    
    try:
        db = MySQLdb.connect("localhost","root","uiop.123.123","tianchi_bigdata" )
        cursor=db.cursor()
    except Exception as error:
        print 'connect eroor'+str(error)
    print 'succed in connecting'
    artist_songs_map={}
    for artist in range(2,40):
        try:
            print artist
            cursor.execute(sql_artist_allsong.format(artistid=artist))
            artist_songs_map[artist]=[item[0] for item in cursor.fetchall()]
        except Exception as error:
            print "got a error in cursor"+str(error)
    print "ok in reading from sql"
    print artist_songs_map
    songs={}
    for artist_id,songlist in artist_songs_map.items():
         print "dealing {art}".format(art=artist_id)
         play_by_day=show_play_byday_artist(songlist)
         ylist=[x[-1] for x in play_by_day]
         songs[artist_id]=ylist
         
    with open('data.json', 'w') as f:
             json.dump(songs, f)    
    print "songs by play"
    print songs     
    db.close()
    