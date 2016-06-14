#coding=utf-8
import toolz
import os
from matplotlib import pyplot as plt
CWD=os.getcwd()
def read_artist(id,taget_dir=None):
    ''' return the data of the artist'''
    if taget_dir:
        try:
            os.chdir(taget_dir)
        except Exception as e:
            print "fail to change dir ,make sure you dir"
    try:
        with open(str(id)+'.csv') as f:
            infor={}
            for line in f:
                item=line.strip("\n").split(",")
                infor[item[0]]=map(float,item[1:])
            
            return infor
    except IOError as fileerror :
        print "can open such file,maybe not exists"
        print fileerror
        
def one(trainlst):
    '''this a pure function,the oupt only depends on input '''
    max_data,min_data=max(trainlst),min(trainlst)
    
    return (map(lambda x:float(x-min_data)/float(max_data-min_data),trainlst),max_data,min_data)

def realsupport(item,last):
    return sum(map(lambda apare:(apare[0]-apare[1])**2,zip(last,item)))

def rangesupport(item,last):
    # item=[1,2,3,4],last=[2,4,6]
    omage=[item[i+1]/item[i] for i in range(len(item)-2) if item[i]!=0.0]
    omage1=[last[i+1]/last[i] for i in range(len(last)-1) if last[i]!=0.0]
    return sum(map(lambda apare:(apare[0]-apare[1])**2,zip(omage,omage1)))


def training(data,n,support=rangesupport):
    ''' traning and predict a value use next(),and n is the length of sliding window'''
    data=list(data)
    onerdata,max_data,min_data=one(data)
    #onerdata=data
    windows=list(toolz.sliding_window(n,onerdata))
    def predict():
        lasted=toolz.tail(n-1,onerdata)
        yingshe={support(item,lasted):item for item in windows}
        minwindow=yingshe.get(min(yingshe.keys()),"1")
        onerdata.append(lasted[-1]*minwindow[-1]/minwindow[-2])
    
       
    while 1:
        predict()
        yield onerdata[-1]*(max_data-min_data)+min_data
    
#def dealwindow(window):

def forcastall(intid):
    data=map(int,read_artist(intid)["action_1"])
    sun=training(data,4)
    fun=toolz.compose(str,int)
    
    predictdata=map(fun,toolz.take(60,sun))    #focast 60 days
    with open("./past_forcast/{aid}.csv".format(aid=intid),"wt") as f:
        f.write(",".join(predictdata))
        
if __name__ == "__main__":
    print "begin"
    print CWD
    for i in range(1,50):
        
       try:
         forcastall(i)
        
       except Exception as e:
           print "deal {i} error".format(i=i)
           print str(e)
           pass
           