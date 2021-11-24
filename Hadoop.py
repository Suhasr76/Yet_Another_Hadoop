import os
import math
import random
#1234
block_size=64 #in bytes for now
path_to_datanodes="C:\\Users\\User\\Desktop\\dfs\\DATANODES"
path_to_namenodes="C:\\Users\\User\\Desktop\\dfs\\NAMENODES"
replication_factor=3
num_datanodes=5
datanode_size=10 #10 blocks in 1 datanode
sync_period=180 #in miliseconds
namenode_checkpoints="C:\\Users\\User\\Desktop\\dfs\\NAMENODES\\CHECKPOINTS"
#1234
#step1
#get input file, calculate size of file, calculate no. of blocks required
def compute_file_size():
    global file_size,number_of_blocks,file_name
    file_name=input("enter the file name along with extension")
    file_size=os.path.getsize(file_name)
    number_of_blocks=math.ceil(file_size/(block_size-1))


#step2
#create X datanodes inside DATANODE directory
#X ranges from 1 to num_datanodes(pre defined variable)
def create_datanodes():
    global datanodes_path_list
    datanodes_path_list=[]
    for i in range(1,num_datanodes+1):
        datanode_name="datanode"+str(i)+".txt"
        datanode_X=path_to_datanodes+str("\\")+datanode_name
        fp=open(datanode_X,"w")
        datanodes_path_list.append([datanode_X,int(0)])
        fp.close()

#step3
#do the same for namenode
def create_namenode():
    global namenode_path
    namenode_path=str(path_to_namenodes+"\\namenode.txt")
    fp=open(namenode_path,"w")
    fp.close()

#step4
#handle Namenode and call BLOCK within
def NAMENODE():



    fp_namenode=open(namenode_path,"w")
    fp_user_file=open(file_name,"r")
    fp_namenode.write(file_name+"###"+str(number_of_blocks)+"\n")
    for i in range(1,number_of_blocks+1):
        flag=True
        while(flag):
            path_offset=random.choice(datanodes_path_list)#optimize
            path=path_offset[0]
            offset=path_offset[1]#check if data node is full, put of 1f(offset<=block_size*10)
            if(offset<=(block_size*datanode_size)-block_size):
                flag=False
        fp_namenode.write("Block"+str(i)+" "+str(path)+" "+"offset: "+str(offset)+" "+"rep_factor: 0"+"\n")
        line=""
        for x in range(block_size-1):
            try:
                line=line+fp_user_file.read(1)
            except:
                break
        offset_new=int(offset)+len(line)+1
        datanodes_path_list[datanodes_path_list.index(path_offset)]=[path,offset_new]
        BLOCK(path,line)

#replication
        lingo_list=[]
        lingo_list.append(path_offset[0])
        rep=1
        while(rep<replication_factor):
            path_offset_new = random.choice(datanodes_path_list)
            if(path_offset_new[0] not in lingo_list):
                lingo_list.append(path_offset_new[0])
                path_new=path_offset_new[0]
                offset_new_x=path_offset_new[1]
                if(offset_new_x<=(block_size*datanode_size)-block_size):
                    BLOCK(path_new,line)
                    fp_namenode.write("Block"+str(i)+" "+str(path_new) +" "+ "offset: " + str(offset_new_x) + " " + "rep_factor: "+str(rep) + "\n")
                    offset_new_x=int(offset_new_x)+len(line)+1
                    datanodes_path_list[datanodes_path_list.index(path_offset_new)] = [path_new, offset_new_x]
                    rep+=1





    fp_namenode.close()
    fp_user_file.close()

def BLOCK(path,line):
    fp_datanode_x=open(path,"a")
    fp_datanode_x.write(line+"\n")
    fp_datanode_x.close()

#call only if HDFS space is bigger than the file to be stored
compute_file_size()
if(math.floor(num_datanodes*block_size*datanode_size/replication_factor)>=file_size):
    create_datanodes()
    create_namenode()
    NAMENODE()
else:
    print("File size too big for HDFS to compute")
