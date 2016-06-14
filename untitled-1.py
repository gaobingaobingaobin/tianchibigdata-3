#coding=utf-8
import re
from functools import wraps
import functools 
import random
def view_wraps(get_method,view_func):
    view_map={method:view for method,view in zip(get_method,view_func)}
    def mywraps(viewfunc):
        @wraps(viewfunc)
        def new_view(request,*args,**argv):
            #print view_map
            if request.method in get_method:
                return view_map[request.method](request,*args,**argv)
            else:
                 return "error method",#"Http404"
        return new_view
    return mywraps

def view_get(request):
    return "getview,you visit"

def view_post(request):
    return "postview"

class get():
    method='GET'
    def __init__(self,maplist):
        for key,value in maplist.items():
            setattr(self,key,value)          
@view_wraps(["GET","POST",],[view_get,view_post],)
def view_all(request):
    return
def view_all_another(request):
    allowedmethod=["GET",'POST']
    def get_view():
        return "you viste get page,and you method is {name}".format(name=request.method)
    def post_view():
        return "you visite post page"
    def wrong_view():
        return "method errior"
    viewlist=[get_view,post_view,]
    mapview=dict(zip(allowedmethod,viewlist))
    if request.method in allowedmethod:
        return mapview[request.method]()
    else:
        return wrong_view()
def sun(a,b,c):
   return a+b+c
      
def minpara(func_1,args):
     num=0
     try:
         func_1(*args)
     except Exception as error:
         num,given=re.findall(r'\d+',str(error))[:2]
     if num:
        #return int(num),int(given)
         return (num,given)
     else:
         #return func(*args)
         return (0,0)

def curried(func):
    #@wraps(func)
    def new_func(*args):
        num,given=minpara(func,args)
        #print num,given
        if (num,given)==(0,0):
            return func(*args)
        else:
            return curried(functools.partial(func,*args))
    return new_func
@curried   
def sun_1(a,b,c):
    return a+b+c
@curried
def sun_2(a,b,c):
     return a*b*c
 
def acollects(a):
    con=[0,]
    def insert(n):
        if len(con)<a:
            con.append(n)
        else:
            con.sort()
            if con[0]<n:
                con[0]=n
    while 1:
        n=yield 
        if isinstance(n,(int,float)):
            insert(n)
        else:
            yield con
        
        
def flatten(nested):
             
    try:
    #如果是字符串，那么手动抛出TypeError。
        if isinstance(nested, str):
            raise TypeError
        for sublist in nested:
        #如果遇到不可迭代的对象也会引发type error
        #yield flatten(sublist)
            #Sprint sublist
            for element in flatten(sublist):
                #yield element
               print('got:', element)
    except TypeError:
            print('here')
            yield nested 
def test():
    x=1.000
    def myadd():
        
        return x
    x+=2
    def show():
        print x
    return [myadd,show]
 
 
 
if __name__=="__main__":
    '''request=get({'a':1})
    print request.method
    print view_all(request)
    print view_all_another(request)'''
    #print minpara(sun,[1,2,])
    print sun_2(1,)(2)(44)
    print sun_1(1,)(2)(44)
    '''print sun_2(1,2)(23)
    test=acollects(10)
    num=xrange(1,1000)'''
    '''next(test)
    for item in num:
         test.send(random.randint(1,98))
        
    print "no error"
    print test.send("stop")'''
    L=['aaadf',[1,2,3],2.0,4.0,[5,[6,[8,[9]],'ddf'],7]]
    for num in flatten(L):
        print(num)    
    print "test enclouse"
    add,show=test()
    print "before add"
    show()
    print "after add"
    add()
    show()
    