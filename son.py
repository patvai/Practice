#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys
from operator import add
import itertools
from itertools import groupby
from itertools import combinations
from operator import itemgetter


from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage is incorrect", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    s=sys.argv[2]
    t=sys.argv[3]
    kd=int(t)
    kl=int(s)
    #lines1 = spark.read.text(sys.argv[2]).rdd.map(lambda g: g[0])
    
    
    lo1=lines.map(lambda line: line.split(",")) \
    .map(lambda line: [line[0],line[1]])
    #print(lo1.collect())
    def gp(l):
        
        final_iterator = []
        for sub,lis in l:
            if len(lis)>=kd:
                #kp=(sub,lis)
                
                final_iterator.append([sub,lis])
        yield final_iterator
    lo2=lo1.groupByKey().map(lambda x: (x[0],list(x[1])))
##    print(lo2.collect())
    lo3=lo2.mapPartitions(gp)
    lo5=lo3.collect()
##    print(lo5)

    def strd(l):
        o=[]
        for i in l:
            for h,g in i:
                f=[]
                for k in g:
                    d=(str(k))
                    f.append(d)
                hi=[str(h),f]
                o.append(hi)
        yield o
    lop=lo3.mapPartitions(strd)
    lol=lop.collect()
    def generatecandidateq(frequent_sets, k):
        combine = set()
        if (k > 2):
            for sets in frequent_sets:
                for s in sets:
                    combine.add(s)
        elif (k == 2):
            for sets in frequent_sets:
                combine.add(sets)
            #print(combine)

        sets = [sorted(x) for x in itertools.chain(*[itertools.combinations(combine, k)])]
        return sets
    def generatecandidate1(frequent_sets, k):
        combine = set()
        if (k > 2):
            for sets in frequent_sets:
                    # for s in sets:
                combine.add(sets)
        elif (k <= 2):
            for sets in frequent_sets:
                combine.add(sets)
            #print(combine)

        sets = [sorted(x) for x in itertools.chain(*[itertools.combinations(combine, k)])]
        return sets
    store16={}
    store15=[]
##    print(lol)
    for i in lol:
        for k,m in i:
##            for h,m in k:
            
            semi=generatecandidate1(m,kd)
            store16=(k,semi)
            store15.append(store16)
##    print(store15)

    stores1=[]

    for i,p in store15:
        
##        for k,v in i:
        for j in p:
            h=[i,j]
                    # print(h)
            stores1.append(h)

            
    #print(stores1)
    stores1.sort(key=itemgetter(1))
    glo1 = [[(x,y) for x, y in g]
               for k, g in groupby(stores1, key=itemgetter(1))]
    #print(glo1)

    storess1=[]
    for i in glo1:
        k=[l[0] for l in i]
        #print(k)
        j=[b[1] for b in i]
        #print(j)
        h=[n for n in j]
##        print(j)
        deva=(k,h[0])
        #print(deva)
        storess1.append(deva)
    #print(storess1)
    storesq1=[]
    for j,m in storess1:
        if len(j)==kl:
            f=(j,m)
            storesq1.append(f)
##    print(storesq1)
    deva1=[x for x in storess1 if x not in storesq1]
    for i, m in deva1:
        ho=list(combinations(i, kl))
        for v in ho:
            good=(list(v),m)
            storesq1.append(sorted(good))
    #print(storesq1)
    sortedp=sorted(storesq1, key=lambda x: x[0])
    store12=[]
    
    for i,k in sortedp:
        if len(i) != 0 and len(k) != 0:
        #jojo=(str(i),str(k))
            t=(", ".join(i))
            liyr="{"+t+"}"
            r=(", ".join(k))
            liyp="{"+r+"}"
        
            pu=(liyr,liyp)
            store12.append(pu)
        else:
            sys.exit("NO SUB GRAPH FOUND AS ZERO IS NOT VALID!!")
##    print(store12)
    if len(store12)==0:
        sys.exit("NO SUB GRAPH FOUND!")
    else:
        for i in  store12:
            koy = ''.join(str(k) for k in i)
            print(koy)
        
        

     
                    

##    lo4=lo3.flatMap(lambda x: x).keys().distinct()
##    lo8=lo4.collect()
##    lo6=lo3.flatMap(lambda x: x[0][1])
##    lo=lo6.collect()
##    graet=", ".join(lo)
##    liyp="{"+graet+"}"
####    print(liyp)
##    lstttt=map(lambda x:str(x),lo)
##    #print(lstttt[1:-1])
##    
####    print(lo2.collectAsMap())
##    li=lo2.groupBy(lambda (x,y): len(y)==kd)
##    #print(li.collect())

    def generatecandidate(frequent_sets):
        
        combine = set()
        if (kl > 2):
            for sets in frequent_sets:
                for s in sets:
                    combine.add(s)
        elif (kl == 2):
            for sets in frequent_sets:
                combine.add(sets)

        sets = [sorted(x) for x in itertools.chain(*[itertools.combinations(combine, kl)])]
        return sets

##    def generatecandidate1(frequent_sets):
##        
##        combine = set()
##        if (kd > 2):
##            for m,sets in frequent_sets:
##                for s in sets:
##                    combine.add(s)
##        elif (kd == 2):
##            for m,sets in frequent_sets:
##                combine.add(sets)
##
##        sets = [sorted(x) for x in itertools.chain(*[itertools.combinations(combine, kd)])]
##        return sets
##    
##    storess=lop.mapPartitions(generatecandidate1)
##    print(storess.collect())
##    store8=lo4.mapPartitions(generatecandidate)
##    
##    
##    #print(store8.collect())
##    lit=[str(r) for r in lo8]
##    store4=generatecandidate(lit)
##    #print(store4)
##    #print("ki")
##    listttt=map(lambda x:'{'+str(x)+'}',lo8)
##    #print(listttt)
##    
##    def groups(l):
##        
##        store9=[]    
##        for i in l:
##            #kf=([str(p) for p in i],[str(l) for l in lo])
##            #t=[str(p) for p in i]
##            t=(", ".join(i))
##            liyr="{"+t+"}"
##            g=[str(l) for l in liyp]
##            r=(liyr,liyp)
##            
##            #print(r)
##            store9.append(r)
##        return store9
####        print(store9)
##    store=store8.mapPartitions(groups)
##    
##    store1=store.collect()
##    #print(store1)
##    #print("ko")
##    for i in store1:
####        l=
##        r=str(i)
##        u=r[1:-1]
##        koy = ''.join(str(k) for k in i)
## ##        print(r[1:-1], end="")
##        
##        #print("hy")
##        istttt=map(lambda x:'{'+str(x)+'}',i)
##        for i in istttt:
##            p=str(i)
##            
##            #print(p[2:-2])
##        
##            
##        
####    store2="".join(store1)
##    #print(store1)
##    lip=map(lambda x:str(x[0]), store1)
##    def change(l):
##        store5=[]
##        for i in l:
##            #print(i)
##            lt=[str(n) for n in i]
##            #print(lt)
##            for k in i:
##                #print(k)
##                p=[str(n) for n in k]
##                #print(p)
####                lizo=map(lambda r:('{'+str(r) for r in k+'}'),k)
####                print(lizo)
####                print("ko")
####                for r in k:
####                    kup=str(r)
######                    print(r)
####                    ju="{"+kup+"}"
####                    #print(ju)
######                liit="{"+str(r) for r in k+"}"
######                print(liit,end="")
####                p=(str(k))
##                store5.append(p)
##        return store5
##    ku=change(store1)
####    print(ku)
####    for i in ku:
####        print(i)
##    def con(k):
##        for i in k:
##            #print(i)
##            return str(i)
##    lop=con(ku)
##    #print(lop)
##    listt=map(lambda x:'{'+str(x)+'}',ku)
##    #print(listt)
##    #print("ho")
##    lt=map(lambda x:str(x[0]),lip)
##    #print(lt)
##    listtt=map(lambda x:'{'+str(x[0])+'}',store1)
    #print(listtt)
    #liy=map(lambda x:(('{'+x[0]+'}'),('{'+x[1]+'}')),store2)
    #print(liy)
##    store10=[]
##    for i in store9:
##        ju=str(i)
##        lp=ju[1:-1]
##        koy = ''.join(str(k) for k in i)
##        print(koy)

        #print("%s" % (koy))
    #print('{1,2}','{"a","b","c"}',end="")
    
##    lo4=lo3.map(lambda (x,y):(x[0],y[0])).collect()
##    lo5=lo3.map(lambda (x,y):(x[1])).collect()
##    print(lo5)
##    lo31=lo3.collect()
##    print(lo31)
##    def groups(l):
##        store=[]
##        for key, group in groupby(lo1, lambda x: x[0]):
##            listOfThings = {thing[1] for thing in group}
##            store.append((key, listOfThings))
##        #store1 = dict(store)
##        yield store
##    lo2=lo1.mapPartitions(groups).collect()

    
##    new=lo2.join(lo1)
##    nw=lo2.join(lo1).collect()
##    #print(nw)
##    values = new.map(lambda (x,y) : y ).collect()
##    va = new.map(lambda (x,y) : y )
##    ho=filter(lambda rec: (rec[1]>=1000000),values)
##    
##    #print(ho)
    
        
        
    
    
##    counts = va.filter(lambda (key, Value): (Value>= 1000000)).map(lambda (x,y): (x, 1)).reduceByKey(add).sortByKey(ascending= True)
##    #counts = va.filter(lambda (key, Value): (Value>= 1000000)).map(lambda (x,y): (x, 1)).reduceByKey(add).groupByKey() 
##                  
##    output1 = counts.collect()
##    #output=filter(lambda rec: (rec[1]>=1000000),output1)
##
##    op = open(sys.argv[3],"w")
##    for (word, count) in output1:
##        if count>=3:
##        
##            line = "%s\t%i" % (word, count)
##            op.write(line)
##            op.write("\n")
    
        
   

    
    spark.stop()
