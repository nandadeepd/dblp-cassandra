import pandas as pd
import numpy
from cassandra.cluster import Cluster
pd.set_option('expand_frame_repr', False)


KEYSPACE = 'testing'


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


def connect(keyspace):
	cluster = Cluster()
	session = cluster.connect(keyspace)
	session.row_factory = pandas_factory
	return session



def findTriangles():

	session = connect(KEYSPACE)

	# Step 1 from the algorithm in docs
	author_rows = session.execute('select * from auth_col_fam')
	publications, main_authors, triangles = list(), list(), list()

	# since rows = panda df, we use iterrows()
	for row in rows.iterrows():
		
		main_authors.append(row['aid'])
		publications.append(row['publications_id'])

	# Step 2
	for idx, publication in enumerate(publications):
		triangle = 0
		# to compute co - authors
		cql_query = 'select %s from publication_col_fam where publications_id = %d'
		query_data = (authors, publication['publication_id'])
		all_authors = session.execute(cql_query, query_data)
		co_authors = all_authors - main_authors[idx]
    # Step 3
		# for that author's co-authors, find their publications to find similarity between that co - author and main author at idx. 
		for each_co_author in co_authors[idx]:
			co_author_publications = session.execute('select %s from auth_col_fam where aid = %d', (publication_id, each_co_author))
			# Step 4:
      if numpy.any(co_author_publications, publications[idx]):
				triangle += 1
				
		triangles.append(triangle)
			

  # Step 5
	most_traingles = main_authors[numpy.argmax(triangles)]

	return most_traingles








