import pandas as pd
import numpy
from cassandra.cluster import Cluster

KEYSPACE = 'cloud_project'
connection= Cluster.connect(KEYSPACE)


au_id= connection.execute(select author_id from author_col_family where author_name = ‘David J. DeWitt’)

all_pub = connection.execute(select pub_id from author_col_family where author_id=au_id)

#For Level-1 Co-authors
l1=[]
for pub in all_pub
	p1=connection.execute(select author_id from publications_col_family where pub_id=pub) 
	l1.append(p1)
level1=list(set(l1))

#For Level-2 Co-authors
l2=[]
all_pub2 = connection.execute(select pub_id from author_col_family where author_id IN level1)

for pub in all_pub2
	p2=connection.execute(select author_id from publications_col_family where pub_id=pub)
	l2.append(p2)

level2= list(set(l2)- level1)

#For Level-2 Co-authors
l3=[]
all_pub3 = connection.execute(select pub_id from author_col_family where author_id IN level2)
for pub in all_pub3
	p3=connection.execute(select author_id from publications_col_family where pub_id=pub)
	l3.append(p3)
l3_intermediate = list(set(l3) - level1)
level3 = list(l3_intermediate - level2)
print len(level3)
