#coding=utf-8

import os
import re
import MySQLdb
def getname(aid):
    
    name,Id=filter(lambda x: aid in x,artist_id_map,)[0]   
    return name

os.chdir(r"E:\data")

def dealfile(filename):
    hashid=getname(re.findall(r"\d+",filename)[0])
    with open(filename) as f:
        for day,play in zip(timelst,f.readline().strip().split(',')):
           tofile.write(','.join([hashid,play,day])+'\n')
def mystr(x):
    if x in range(0,10):
        return "0"+str(x)
    else:
        return str(x)
    
timelst=['20150831',]+["2015"+'09'+mystr(day) for day in range(1,31)]+["2015"+'10'+mystr(day) for day in range(1,31)]


if __name__=="__main__":
    try:
        db = MySQLdb.connect("localhost","root","uiop.123.123","tianchi_bigdata")
        cursor=db.cursor()
        cursor.execute('select * from artists_id_map')
        artist_id_map=cursor.fetchall()
    except Exception as error:
        print "got a error"
    print os.listdir('.')
    with open("mars_tianchi_artist_plays_predict.csv","wt") as tofile:
        for name in filter(lambda x:len(x)<10,os.listdir('.')):
           if ".csv" in name:
              dealfile(name)