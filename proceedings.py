from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('dblp')
all_procs=session.execute('select proc_id,auth_ids from procd where year=2005 allow filtering')
temp_max=0
proc_list=[]
for proc in all_procs:
    if(len(proc.auth_ids)>temp_max):
        proc_list=[]
        proc_list.append(proc.proc_id)
        temp_max=len(proc.auth_ids)
    elif(len(proc.auth_ids)== temp_max):
        proc_list.append(proc.proc_id)

for j in proc_list:
    res1= session.execute('select proc_id,pub_type,title,year,conference from procd where proc_id = %s',(j,))
for res2 in res1:
    print res2


