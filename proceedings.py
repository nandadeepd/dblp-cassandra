all_proc=connection.execute(select proc_id,author_ids from proceedings)
#storing temporary maximum,and their ids
temp_max=0
proc_list=[]
#count the size of the author_ids array and find the publications with maximum count
for proc in all_proc{
	if(len(proc.author_ids)>temp_max){
		proc_list.clear()
		proc_list.append(proc.proc_id)
		temp_max=len(proc.author_ids)
	}
#handles the case where two proceedings has the maximum count
	else if(len(proc.author_ids)=temp_max){
		proc_list.append(proc.proc_id)
	}
}
#for each of the proc_id print its details
for i in proc_list
{
	result=connection.execute(select proc_id,title from proceedings where proc_id=%i)
	print result
}
