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

from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('dev')
insert_sql = ("INSERT INTO emp (empid, name) "
              "VALUES(%s, %s) ")
name= "sample"
insert_data = (123, name)
session.execute(insert_sql, insert_data)

rows = session.execute('select * from emp')
print([row for row in rows])
