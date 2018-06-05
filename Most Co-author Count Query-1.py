# Which publication has the most co-authors? Give full information about the paper, including title, authors and venue.
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('dblp')

all_pub = session.execute('select pub_id,auth_ids from pub_cf')

temp_max=0
pub_list=[]

for pub in all_pub:
    if(len(pub.auth_ids) > temp_max):
        pub_list=[]
        pub_list.append(pub.pub_id)
        temp_max=len(pub.auth_ids)
    elif(len(pub.auth_ids)== temp_max):
        pub_list.append(pub.pub_id)
        temp_max=len(pub.auth_ids)

for j in pub_list:
     res1= session.execute('select pub_id,pub_type,title,year from pub_cf where pub_id = %s',(j,))
for res2 in res1:
    print res2

