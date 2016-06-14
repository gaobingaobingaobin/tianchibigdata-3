import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import csv
import MySQLdb
import time
mars_tianchi_songs='p2_mars_tianchi_songs.csv'
user_actions='p2_mars_tianchi_user_actions.csv'

class grow_dict():
    def __init__(self,first_id):
        self.pool={}
        self.first_id=first_id
        self.next_id=first_id
    
    def get(self,key):
        if key not in self.pool:
            self.pool[key]=self.next_id
            self.next_id+=1
        return self.pool.get(key,'Error')
                                  
def deal_mars_tianchi(filename):
    with open(filename, 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=' ')
        
        for row in spamreader:
            song_id,artist_id,publish_time,song_init_plays,Language,Gender=row[0].split(',')
            song_id,artist_id=song_id_pool.get(song_id),artist_id_pool.get(artist_id)
            yield (str(song_id),str(artist_id),publish_time,song_init_plays,Language,Gender)
            
def deal_user_actions_tianchi(filename):
    with open(filename, 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=' ')
        for row in spamreader:
            user_id,song_id,gmt_create,action_type,Ds=row[0].split(',')
            user_id,song_id=user_id_pool.get(user_id),song_id_pool.get(song_id)
            yield (str(user_id),str(song_id),gmt_create,action_type,Ds)
            
              

                     
if __name__ =="__main__":
    begin=time.time()
    song_id_pool=grow_dict(1)  # init the pool of songs id
    artist_id_pool=grow_dict(1) #init the pool of artist id
    user_id_pool=grow_dict(1)  #init the pool of user id
    songs_data=deal_mars_tianchi(mars_tianchi_songs)
   
    try:
        db = MySQLdb.connect("localhost","root","uiop.123.123","tianchi_bigdata" )
        print "succed to connect to local database"
    except Exception as e:
        print 'a error in connecting to local database plz config your parameters'
        print str(e)
     
            
               
    cursor = db.cursor()
            
    sql_songs='insert into p2_mars_tianchi_songs values("%s","%s","%s","%s","%s","%s")'
    sql_user_actions='insert into p2_mars_tianchi_user_actions values("%s","%s","%s","%s","%s")'
    sql_song_id_pool='insert into p2_songs_id_map values("%s","%s")'
    sql_user_id_pool='insert into p2_user_id_map values("%s","%s")'
    sql_artist_id_map='insert into p2_artists_id_map values("%s","%s")'
    print 'insert songs'
    count=0
    for line in songs_data:
           
           try:
                cursor.execute(sql_songs%line)
                if count%10000==0:
                    db.commit()
           except Exception as error:
               print 'insert error'+str(error)
               
    db.commit()          
    print 'insert actions'
    actions=deal_user_actions_tianchi(user_actions)
    count=0
    for line in actions:
            
            count+=1
            try:
                cursor.execute(sql_user_actions%line)
                if count%10000==0:
                   db.commit()
            except Exception as error:
                    print 'insert error'+str(error)
                    
    db.commit()
    print 'insert mapping pools infor to sql'
    for item in song_id_pool.pool.items():
        try:
            cursor.execute(sql_song_id_pool%item)
            
        except Exception as em:
            print 'insert error'+str(em)
    db.commit()
    
    print 'songs_id_map ok'
    for item in artist_id_pool.pool.items():
        try:
            cursor.execute(sql_artist_id_map%item)
        except Exception as err:
            print 'insert error'+str(err)
    db.commit()
    print 'artists_id_map ok'
    count=1
    for item in  user_id_pool.pool.items():
        count+=1
        try:
            cursor.execute(sql_user_id_pool%item)
            if count%3000==0:
                db.commit()
            
        except Exception as error:
            print 'insert error'+str(error)
    
    
    db.commit()
    
    
    
    
    
    print 'insert finished'
    print 'total time is '+str(time.time()-begin)
    db.close()
            
    
    
    
    
       