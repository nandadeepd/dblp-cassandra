'''
Installation guide:

1. install brew 
2. pip install cql
3. brew install cassandra

open new terminal 

4. type cqlsh to test 
5. create a keyspace - in this case, dev is a keyspace and the snippet below connects 
to dev keyspace - so change accordingly. 
6. Run this script. 
'''

import pandas as pd
from cassandra.cluster import Cluster
pd.set_option('expand_frame_repr', False)

KEYSPACE = 'dev'


def connect(keyspace):
	cluster = Cluster()
	session = cluster.connect(keyspace)
	return session

def insert(session, name, some_id):

	insert_sql = ("INSERT INTO emp (empid, name) "
	              "VALUES(%s, %s) ")
	# name= "sample"
	insert_data = (some_id, name)
	session.execute(insert_sql, insert_data)
	return 1

def fetch(session):
	return session.execute('select * from emp')


readFile = lambda path : pd.read_csv(path, header = 'infer')


def manageDBLP():
	dblp_dataset = readFile('dblp-fraction.csv')
	dblp_authors_groups = dblp_dataset.groupby(['author_name'])
	# print(dblp_authors_groups.count())

	for key, item in dblp_authors_groups:
		print (dblp_authors_groups.get_group(key))
		print("\n")
		print("\n")
	# here, if we create an author keyspace, we would ideally insert each row with author as key
	# and the other params such as publications, co-authors, etc. 


def main():

	session = connect(KEYSPACE)
	success = insert(session, "John Doe", 456)

	print([row for row in fetch(session)])

if __name__ == '__main__':
	main()


