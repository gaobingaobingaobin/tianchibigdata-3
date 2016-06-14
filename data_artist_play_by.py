# -*- coding: utf-8 -*-
from matplotlib import pyplot as plt
import os
import json
import MySQLdb

import itertools

def play_byday_artist(artist_id):
    ''' input the id artist,the output is the play times by day of the artist'''
    songsall=artist_song_map[artist_id]
    def song_day(songs):
        for song in songs:
            for item in song_user_actions.get(song,''):
                yield item
    all_actions=sorted(song_day(songsall),key=lambda x:int(x[-1]))
    group={key:len(list(values)) for key,values in itertools.groupby(all_actions,key=lambda x:int(x[-1]))}
    ylist=[play for day,play in sorted(group.items(),key=lambda x:x[0])]  #only return the play times can keep day if necessary
    return ylist
    
                                                                                    
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
    db.close()
    print "ok in get data"
    mars_tianchi_user_actions=filter(lambda x:x[-2]=="1",mars_tianchi_user_actions)  #save those actions types=1 chose actions here
    artist_song_map={key:map(lambda x:x[0],value)  for key,value in itertools.groupby(mars_tianchi_songs,key=lambda x:x[-1])}
    song_user_actions={key:list(value) for key,value in itertools.groupby(mars_tianchi_user_actions,key=lambda x:x[1])}
    print 'play times of artist 3'
    with open("play_times_byday_artist_like.csv","wt") as f:
        for artist in artist_song_map.keys():
            print "dealing ",artist
            playtimes=play_byday_artist(artist)
            f.write('artist'+str(artist)+',')
            f.write(','.join(map(str,playtimes)))
            f.write('\n')
            
            