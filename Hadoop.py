import os
import math
import random

block_size=64 #in bytes for now
path_to_datanodes="C:\\Users\\User\\Desktop\\dfs\\DATANODES"
path_to_namenodes="C:\\Users\\User\\Desktop\\dfs\\NAMENODES"
replication_factor=3
num_datanodes=5
datanode_size=10 #10 blocks in 1 datanode
sync_period=180 #in miliseconds
namenode_checkpoints="C:\\Users\\User\\Desktop\\dfs\\NAMENODES\\CHECKPOINTS"

#step1
#get input file, calculate size of file, calculate no. of blocks required
file_name=input("enter the file name along with extension")
file_size=os.path.getsize(file_name)
number_of_blocks=math.ceil(file_size/block_size)

#step2
#create X datanodes inside DATANODE directory
#X ranges from 1 to num_datanodes(pre defined variable)
datanodes_path_list=[]
for i in range(1,num_datanodes+1):
    datanode_name="datanode"+str(i)+".txt"
    datanode_X=path_to_datanodes+str("\\")+datanode_name
    fp=open(datanode_X,"w")
    datanodes_path_list.append([datanode_X,int(0)])
    fp.close()

#step3
#do the same for namenode
namenode_path="C:\\Users\\User\\Desktop\\dfs\\NAMENODES\\namenode.txt"
fp=open(namenode_path,"w")
fp.close()

#step4
#handle Namenode and call BLOCK within
def NAMENODE():


    global namenode_path,datanodes_path_list,file_name,number_of_blocks,block_size,replication_factor
    fp_namenode=open(namenode_path,"w")
    fp_user_file=open(file_name,"r")
    fp_namenode.write(file_name+"###"+str(number_of_blocks)+"\n")
    for i in range(1,number_of_blocks+1):
        path_offset=random.choice(datanodes_path_list)
        path=path_offset[0]
        offset=path_offset[1]
        fp_namenode.write(str(path)+" "+"offset: "+str(offset)+" "+"rep_factor: 0"+"\n")
        line=""
        for x in range(block_size-1):
            try:
                line=line+fp_user_file.read(1)
            except:
                break
        offset_new=int(offset)+len(line)+1
        datanodes_path_list[datanodes_path_list.index(path_offset)]=[path,offset_new]
        BLOCK(path,line)



    fp_namenode.close()
    fp_user_file.close()

def BLOCK(path,line):
    fp_datanode_x=open(path,"a")
    fp_datanode_x.write(line+"\n")
    fp_datanode_x.close()
NAMENODE()

