# Which publication has the most co-authors? Give full information about the paper, including title, authors and venue.

import pandas as pd
import numpy
from cassandra.cluster import Cluster

KEYSPACE = 'cloud_project'
connection= Cluster.connect(KEYSPACE)

all_pub = connection.execute(select pub_id,author_ids from publications_col_family)

temp_max=0
pub_list=[]

for pub in all_pub
	if(pub.author_ids.size() > temp_max)
		pub_list.clear()
		pub_list.append(pub.pub_id)
		temp_max=pub.author_ids.size()
	else if (pub.author_ids.size() == temp_max)
		pub_list.append(pub.pub_id) #display both the publication ids if the number of co-authors in both publications are equal

for i in pub_list
	result = connection.execute(select pub_id, title, author_ids from publications_col_family where pub_id='i')
	print result