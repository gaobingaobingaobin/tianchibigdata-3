# -*- coding: utf-8 -*-
from matplotlib import pyplot as plt
import os
import json
import MySQLdb
import toolz
import itertools
from multiprocessing.dummy import Pool as ThreadPool
pool = ThreadPool(processes=8) #init tread ppol
os.chdir("E:\data\song_info")
song_attrs={"artist_id":"","init_play":"","publish_time":"","language":"","play_times":"","hot":"",}
timelst='''20150301,20150302,20150303,20150304,20150305,20150306,20150307,20150308,20150309,20150310,20150311,20150312,20150313,20150314,20150315,20150316,20150317,20150318,20150319,20150320,20150321,20150322,20150323,20150324,20150325,20150326,20150327,20150328,20150329,20150330,20150331,20150401,20150402,20150403,20150404,20150405,20150406,20150407,20150408,20150409,20150410,20150411,20150412,20150413,20150414,20150415,20150416,20150417,20150418,20150419,20150420,20150421,20150422,20150423,20150424,20150425,20150426,20150427,20150428,20150429,20150430,20150501,20150502,20150503,20150504,20150505,20150506,20150507,20150508,20150509,20150510,20150511,20150512,20150513,20150514,20150515,20150516,20150517,20150518,20150519,20150520,20150521,20150522,20150523,20150524,20150525,20150526,20150527,20150528,20150529,20150530,20150531,20150601,20150602,20150603,20150604,20150605,20150606,20150607,20150608,20150609,20150610,20150611,20150612,20150613,20150614,20150615,20150616,20150617,20150618,20150619,20150620,20150621,20150622,20150623,20150624,20150625,20150626,20150627,20150628,20150629,20150630,20150701,20150702,20150703,20150704,20150705,20150706,20150707,20150708,20150709,20150710,20150711,20150712,20150713,20150714,20150715,20150716,20150717,20150718,20150719,20150720,20150721,20150722,20150723,20150724,20150725,20150726,20150727,20150728,20150729,20150730,20150731,20150801,20150802,20150803,20150804,20150805,20150806,20150807,20150808,20150809,20150810,20150811,20150812,20150813,20150814,20150815,20150816,20150817,20150818,20150819,20150820,20150821,20150822,20150823,20150824,20150825,20150826,20150827,20150828,20150829,20150830'''.split(",")


def tofile(myitem):
    artist_id,songsinfor=myitem
    with open(artist_id+".json","wt") as afile:
        json.dump(songsinfor,afile)
   
def mydivison(item):
    if item[0]==0:
        return 0
    else:
        return float(item[1])/float(item[0])
    
def deal_songs(song_item):
    ''' this is a impure func depends on global vars likesong_playmap  '''
    song_id,artist_id,publish_time,song_init_plays,Language,Gender=song_item
    print "song_id is {songid}".format(songid=song_id)
    play_map=filter(lambda x:x[1]=="1",songs_play_keymap.get(song_id,[(1,2,3),]))
    '''download_map=filter(lambda x:x[1]=="2",songs_play_keymap.get(song_id,[(1,2,3),]))
    like_map=filter(lambda x:x[1]=="3",songs_play_keymap.get(song_id,[(1,2,3),]))'''
    if  not play_map:   # if has not beens played
        return {"artist_id":artist_id,"init_play":song_init_plays,"publish_time":publish_time,"language":Language,"play_lst":[0,]*183,"hot":[0,]*183,"song_id":song_id}
    
    #actions_map=toolz.groupby(lambda x:x[1],play_map,)#groupby action_type
    #actions_maplist=(actions_map[im] for im in ["1",] )
    actions_maplist=[play_map,] # filter those play actions
    def colections(item):
        sun={key:0 for key in timelst}
        count_dict={key:len(values)for key,values in toolz.groupby(lambda x:x[-1],item).items()} #actions groupby DS count play time of each day
        sun.update(count_dict)
        return sun
         
    actions_time_list=map(colections,actions_maplist)
    playlst=[ actions_time_list[0].get(akey,0) for akey in sorted(actions_time_list[0].keys()) ]
    #print playlst
    playall=[sum(playlst[:i]) for i in range(1,len(playlst))]
    #print playall
    hot=map(mydivison,enumerate(playall,1))

    return {"artist_id":artist_id,"init_play":song_init_plays,"publish_time":publish_time,"language":Language,"play_lst":playlst,"hot":hot,"song_id":song_id}
     
        
if __name__=="__main__":
    
    try:
        db = MySQLdb.connect("localhost","root","uiop.123.123","tianchi_bigdata" )
        cursor=db.cursor()
    except Exception as error:
        print 'connect eroor'+str(error)
    orginal_songs_artist_map_sql='select * from p2_mars_tianchi_songs' #选歌曲id和艺术家id，按艺术家id进行排序
    orginal_user_actions='select song_id,action_type,Ds from p2_mars_tianchi_user_actions order by song_id' #选取用户id，歌曲id，操作类型，日期，并且按照歌曲id进行排序
    try:
        cursor.execute(orginal_songs_artist_map_sql)
        mars_tianchi_songs=cursor.fetchall()
        cursor.execute(orginal_user_actions)
        mars_tianchi_user_actions=cursor.fetchall()
    except Exception as error:
            print 'curosr eroor'+str(error)
    print len(mars_tianchi_songs)
    print len(mars_tianchi_user_actions)
    songs_play_keymap=toolz.groupby(lambda x:x[0],mars_tianchi_user_actions) #groupby song_id
    #print timelst
    print len(songs_play_keymap)
    print "dealing songs,just waitting"
    song_dict_list=map(deal_songs,mars_tianchi_songs)
    map_artist=toolz.groupby(lambda x:x.get("artist_id"),song_dict_list)
    print len(song_dict_list)
    print song_dict_list[3]
    print len(map_artist["2"])
    pool.map(tofile,map_artist.items())  #write file with multi threadings

    db.close()
