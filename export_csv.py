# -*- coding: utf-8 -*-
import numpy as np
import MySQLdb

try:
    db = MySQLdb.connect("localhost","root","uiop.123.123","tianchi_bigdata" )
    cursor=db.cursor()
except Exception as error:
        print 'connect eroor'+str(error)

cursor.execute("select * from user_id_map")
data=cursor.fetchall()
print "get ok"
with open("D:\\user_id_map.csv","w") as f:
    for line in data:
       f.write(','.join(list(line))+'\n')

db.close()
        