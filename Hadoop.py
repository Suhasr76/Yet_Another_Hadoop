import os
import math
file_name=input("enter the file name along with extension")
file_size=os.path.getsize(file_name)
#you are supposed to get "block_size" value from json file
block_size=400
block_number=math.ceil(file_size/block_size)
print(file_size)

def Name_Node():
    global block_size,block_number,file_name
    fp_name_node=open("name_node.txt","w")
    fp_name_node.write(str(block_number)+"\n")
    fp_user_file=open(file_name,"r")
    for block in range(block_number):
        fp_name_node.write(str(block+1)+" "+"block"+str(block+1)+".txt"+"\n")
        line=""
        for x in range(block_size):
            try:
                line=line+str(fp_user_file.read(1))
            except:
                break
        Data_Node(line,block)
    fp_name_node.close()
    fp_user_file.close()
def Data_Node(line,block):
    block_name="Block"+str(block+1)+".txt"
    fp_data_node=open(block_name,"w")
    fp_data_node.write(line)
    fp_data_node.close()

Name_Node()