import pandas as pd
import numpy
from cassandra.cluster import Cluster
cluster=Cluster()
session= cluster.connect('dblp')
name="David J. DeWitt"
au_id= session.execute('select auth_id,pub_ids from auth_cf where "auth_name" = %s allow filtering',(name,))

res=au_id[0]
list,l1,l2,l3= [],[],[],[]
list=res.pub_ids
author_id = [res.auth_id]
level1,level2,level3=[],[],[]
for i in list:
    a1=session.execute('select auth_id from pub_cf where "pub_id"=%s',(i,))
    a=[]
    a=a1[0]
    for j in a.auth_id:
    	l1.append(j)
level1=set(l1)
print "Level 1 co-author count: " + str(len(level1))

#For Level-2 Co-authors
l,all_pub1,all_pub2=[],[],[]
for i in level1:
    pub1=session.execute('select pub_ids from auth_cf where "auth_id"=%s',(i,))
    p=[]
    p=pub1[0]
    for j in p.pub_ids:
    	l.append(j)
all_pub1=set(l)

for pub in all_pub1:
	a2=session.execute('select auth_id from pub_cf where "pub_id"=%s',(pub,))
	a=[]
	a=a2[0]
	for j in a.auth_id:
		l2.append(j)
level2= set(l2)- level1
print "Level 2 co-author count: " + str(len(level2))
#For Level-3 Co-authors
l,l3_intermediate=[],[]
for i in level2:
    pub2=session.execute('select pub_ids from auth_cf where "auth_id"=%s',(i,))
    p=[]
    p=pub2[0]
    for j in p.pub_ids:
    	l.append(j)
all_pub2=set(l)

for pub in all_pub2:
	a3=session.execute('select auth_id from pub_cf where "pub_id"=%s',(pub,))
	a=[]
	a=a3[0]
	for j in a.auth_id:
		l3.append(j)
l3_intermediate = set(l3) - level1
level3 = l3_intermediate - level2 

print "Level 3 co-author count: " + str(len(level3))

