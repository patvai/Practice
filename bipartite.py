#PYTHON 3.7 IMPLEMENTATION
import sys
from itertools import groupby
from itertools import combinations
import itertools
from operator import itemgetter
#from itertools import groupby

import string
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage is incorrect")
        sys.exit(-1)

    program_name = sys.argv[0]
    file1 = sys.argv[1]
    s = sys.argv[2]
    t = sys.argv[3]
    kd=int(t)
    kg=int(s)
    # print(s)
    # print(t)
    store = []
    with open(file1, 'r') as f:

        lines = f.readlines()
        #print(lines)
        io = []

        for i in lines:
            io.append(i.rstrip().split(','))
        io1=sorted(io)
        # print(io1)

        lp=[list(item) for item in set(tuple(row) for row in io)]
        # print(lp)
        # for i in io:
        #     koh=set(i)
        #     print(koh)
        # # io1=set(io)
        # from collections import defaultdict
        #
        # d = defaultdict(list)
        # for k, v in io1:
        #     d[k].append(v)
        # print(dict(d))
        for key, group in groupby(io1, lambda x: x[0]):
            listOfThings = {thing[1] for thing in group}
            store.append((key, listOfThings))
        store1 = dict(store)
        # print(store)
        # print(store1)
        # for i,j in store1.items():
        #     print(len(j))

        # print(store1)
        st={}

        for l,k in store1.items():
            if len(k)>=kd:
                st[l]=k
        # print(st)

        def check(D):
            p=[]
            for k,i in D.items():
                h=(k,list(i))
                u=list(h)
                p.append(u)
            return p
        sod=check(st)
        # print(sod)
        def chsub(l):
            d=[]
            for i in l:
                lst3 = [list(filter(lambda x: x[1] in l, sublist)) for sublist in l]
                d.append(lst3)
            return d
        # print(chsub(sod))


        def generatecandidate(frequent_sets, k):
            combine = set()
            if (k > 2):
                for sets in frequent_sets:
                    for s in sets:
                        combine.add(s)
            elif (k == 2):
                for sets in frequent_sets:
                    combine.add(sets)
            #print(combine)

            sets = [set(sorted(x)) for x in itertools.chain(*[itertools.combinations(combine, k)])]
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
            # elif (k==0):
            #     for sets in frequent_sets:
            #         combine.add(0)
            #print(combine)

            sets = [set(sorted(x)) for x in itertools.chain(*[itertools.combinations(combine, k)])]
            return sets

        ## writing code to enhance!!
        store11={}
        store12=[]
        for k,j in store1.items():
            if len(j)>=kd:
                store11[k]=j
        store12.append(store11)
        # print(store12)

        store15=[]
        store16={}
        for i in store12:
            for j,k in i.items():
                li=list(k)
                # print(li)
                semi=generatecandidate1(li,kd)
                # print(semi)
                store16[j]=semi
        store15.append(store16)
        # print(store15)
        store17=[]
        store18={}
        stores=[]
        store19=[]
        for i in store15:
            for h,m in i.items():
                store17.append(h)

        #print(store17)        # print(list(combinations(tl, kg)))

        do=generatecandidate(store17,kg)
        storess=[]
        for i in do:
            h=list(i)
            storess.append(h)
        # print(storess)

        storea=[]
        for i,m in store1.items():
            if len(m)>=kd:
                g=[i,list(m)]
                storea.append(g)
        # print(store15)

        storep=[]
        stored=[]
        storew={}
        storeq=[]
        for i in store15:
            for h,v in i.items():
                r=list(h)
                storep.append(h)
                d=generatecandidate1(storep,kg)
                for i in d:
                    for f in v:

                        ho=(i,f)
                        storeq.append(ho)
                # go=(d,v)
                # print(go)
        # print(storeq)
        stored=[]
        for i,k in storeq:
            to=list(k)
            r=list(i)
            gf=(r,to)
            #ih=dict(gf)
            stored.append(gf)
            #print(gf)
        # print(stored)
        stores1=[]

        for i in store15:
            for k,v in i.items():
                for j in v:
                    h=[k,list(j)]
                    # print(h)
                    stores1.append(h)

        # print(stores1)
        stores1.sort(key=itemgetter(1))
        # print(stores1)
        glo1 = [[(x,y) for x, y in g]
               for k, g in groupby(stores1, key=itemgetter(1))]
        # print(glo1)
        # print("hi")
        storess1=[]
        for i in glo1:
            k=[l[0] for l in i]
            # print(k)
            j=[b[1] for b in i]
            # print(j)
            h=[n for n in j]
            # print(h[0])
            deva=(k,h[0])
            # print(deva)
            # print("ji")
            storess1.append(deva)
        # print(storess1)

        storesq1=[]

        def leng(l):

            return len(l)
        # print(leng(storess1))

        for j,m in storess1:
            if len(j)==kg:
                f=(j,m)
                storesq1.append(f)
        # print(storesq1)
        # print("ko")
        deva1=[x for x in storess1 if x not in storesq1]
        for i, m in deva1:
            ho=list(combinations(i, kg))
            for v in ho:
                good=(list(v),m)
                storesq1.append(good)
        sortedp=sorted(storesq1, key=lambda x: x[0])

        # msg=[]
        # def check(l):
        #
        #     for i, k in sortedp:
        #         u=len(i)
        #         j=len(k)
        #         msg.append([u,j])
        #     return msg
        #
        #
        # msg.append("No Sub Graph as size 0 is not valid")
        #


        # print(sortedp)
        # for i, k in sortedp:
        #     if len(i) == 0 or len(k) == 0:
        #         k+=0
        #     print("No sub graph found.")
        #
        #
        #

        # if [len(i[0]) for i in sortedp]==0 or [len(i[1]) for i in sortedp]==0:
        #     print("No sub graph found.")

        if len(storesq1)==0:

            sys.exit("No sub graph found!")
        # elif :

        # elif for
        else:
            for i, k in sortedp:
                # try:
                if len(i) != 0 and len(k) != 0:
                    jojo = (set(i), set(k))

                    # print(jojo)
                    koy = ''.join(str(k) for k in jojo)
                    print(koy)

                else:
                    sys.exit("No sub graph found as zero is not valid!")


                # except:
                #     print("No sub graph found.")

        # if for i, k in sortedp:
        #
        #
        #     if len(i)==0 or len(k)==0:
        #         print("No sub graph found.")


                # print(i,k)



            # print(jojo)








                # k=j[0]
                # n=j[1]
                # print(k,n)
            # print(i)





        # for i,j in stores1:
        #     print(i)
                    # h.sort(key=itemgetter(1))
                    #
                    # glo = [[x for x, y in g]
                    #        for k, g in groupby(storea, key=itemgetter(1))]

                    # g=list(filter(lambda x: x[1]==x[1], h))
                    # print(g)

        # for i in stored:
        #     j=i[0]
        #     h=i[1]
        #     # print(h)
        #     for l in j:
        #         for n in sod:
        #             f=n[0]
        #             l=n[1]
        #             if l==f:
        #                 tg=list(set(h) & set(l))
        #                 print(tg)



                        # for i in stored:
        #     # print(i)
        # for f in store15:
        #     for u,v in f.items():
        # kj={}
        # for i,m in stored:
        #     for d in i:
        #         for g in store15:
        #             for k,v in g.items():
        #                 for t in v:
        #                     jh=list(t)
        #                     # if set(jh)<=set(m):
        #                     #     print(m)
        #
        #                     # print(t)
        #                     if g[d]==m:
        #                         print(g[d])




                        # for l in v:
                        #     ty=list(l)





                                # kj[i]=m










        #stored.append(d)

                #print("hi")



                #print(r)


            # for d,v in i:
            #     print(d)


        #
        # #values = set(map(lambda x: x[1], storea))
        # storea.sort(key=itemgetter(1))
        #
        # glo = [[x for x, y in g]
        #        for k, g in groupby(storea, key=itemgetter(1))]



        # print(storea)
        # print("ko")





        # for i in store15:
        #     for l,j in i.items():





        # for y in store15:
        #     for k,l in y.items():








        #for j,k in store11.items():





        #
        # to = []
        # for key, value in store1.items():
        #     if len(value) == kd:
        #         # print(value)
        #         to.append((key, value))
        # #print(to)
        # k1 = dict(to)
        # # print(k1)
        # lo = []
        # ko = []
        # for i, v in k1.items():
        #     o = set(v)
        #     lo.append(v)
        #     ko.append(i)
        #
        # # print({i}, v, end="")
        #
        #
        # # # print(ko)
        # # import itertools
        #
        #
        #
        #
        # # print('hi')
        # store8=generatecandidate(ko,kg)
        # #print(store8)
        # store9=[]
        # for i in store8:
        #     kf=i,lo[0]
        #     store9.append(kf)
        # #print(store9)
        #
        # # print(koy)
        # store10=[]
        # for i in store9:
        #     ju=str(i)
        #     lp=ju[1:-1]
        #     koy = ''.join(str(k) for k in i)
        #     # print(koy)
        #
        #
        #     # s = ju[:kg] + "" + ju[kg + 1:]
        #
        #     # print(s)
        #
        #     # print(l,end="")




            # for j in i:
            # kp=str(i).replace(",","",).replace(" ","")
            # store10.append(kp)

        # for i in store10:
            # print(i)






        # y=[]
        # comb = list(combinations(ko, kg))
        # print(comb)
        # yu=[[item] for item in comb]
        # print(yu)
        # print("koi")
        # kdr=dict(yu)
        # print(kdr)
        #
        #
        #
        # dictOfWords = {yu[i] for i in range(0, len(yu))}
        # print(dictOfWords)
        # for i in dictOfWords:
        #     print(i,lo[0])
        # # for i in list(comb):
        # #     print(i)
        #     # st=" "
        # store7 = []
        # lp = dictOfWords, lo[0]
        # kio = store7.append(lp)
        # # print(dictOfWords,lo[0])
        # for i in lp:
        #     print(i,end="")
        #

















