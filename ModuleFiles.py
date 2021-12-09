from getpass import getuser
import json
import os
import math
import random
import sys
import os
import math
import random
import collections
import itertools
import multiprocessing
import operator
import glob
import multiprocessing
import string
import sys
import importlib
# sys.path.insert(1,'/Users/jeshwanth/Desktop')
#import mapper
# sys.path.insert(1,'/Users/jeshwanth/Desktop')
#import reducer

class MapReduce(object):
    
    def __init__(self, map_func, reduce_func, num_workers=None):#Activated when we create the class object
        """
        map_func
          mapper file Function
        
        reduce_func
           reducer file function
         
        num_workers
          The number of workers to create in the pool. Automatically Defaults to the
          number of CPUs available on the system.
        """
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.pool = multiprocessing.Pool(num_workers)#Assigning the number of processes(Concurrent execution) u want, Default is your CPU cores
    
    def partition(self, mapped_values):
        """Organize the mapped values by their key.
        Returns an unsorted sequence of tuples with a key and a sequence of values.
        
        [('money', [1]), ('over', [1, 1]), ('centralized', [1]), ('ledger', [1, 1]), ('paper', [1]), ('through', [1]), ('validators', [1]), ('issuer', [1]), ('proportion', [1]), ('to', [1, 1, 1, 1, 1, 1, 1, 1]), ('issued', [1, 1]), ('first', [1, 1]), ('return', [1]), ('cryptography', [1]), ('get', [1, 1]), ('records', [1, 1]), ('they', [1, 1, 1]), ('not', [1, 1, 1]), ('using', [1]), ('bank', [1]), ('like', [1]), ('generally', [1, 1]), ('crypto', [1, 1, 1]), ('serves', [1]), ('token', [1, 1, 1]), ('stored', [1]), ('each', [1]), ('release', [1]), ('works', [1]), ('schemes', [1]), ('financial', [1]), ('creation', [1]), ('some', [1]), ('authority', [1, 1]), ('individual', [1]), ('are', [1, 1, 1]), ('implemented', [1]), ('stakers', [1]),....)]
        
        
        """
        partitioned_data = collections.defaultdict(list)#Defaultdict returns a dictionary-like objectwhere a keyerror corresponds to a list in this case
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return(sorted(partitioned_data.items(),key=lambda kv: kv[0],reverse=False))
        #return partitioned_data.items()#items() method is used to return the list with all dictionary keys with values.
    
    def __call__(self, inputs, chunksize=1):#Activated when we call the object of the class
        """Process the inputs through the map and reduce functions given.
        
        inputs
          An iterable containing the input data to be processed.
        
        chunksize=1
          The portion of the input data to hand to each worker.  This
          can be used to tune performance during the mapping phase.
        """
        map_responses = self.pool.map(self.map_func, inputs, chunksize=chunksize)#Multithreading the mapper part
        partitioned_data = self.partition(itertools.chain(*map_responses))#combine all and pass it to partition function
        reduced_values = self.pool.map(self.reduce_func, partitioned_data)##Multithreading the reducer part
        return reduced_values


# sys.path.insert(1,'/Users/jeshwanth/Desktop')
#1234
config = "config/test_config.json"
f = open(config)
data = json.load(f)
block_size = data['block_size']
path_to_datanodes = data['path_to_datanodes'].replace('$USER',getuser())
if not os.path.isdir(path_to_datanodes):
    os.mkdir(path_to_datanodes)
path_to_namenodes = data['path_to_namenodes'].replace('$USER',getuser())
if not os.path.isdir(path_to_namenodes):
    os.mkdir(path_to_namenodes)
replication_factor=3
num_datanodes = data['num_datanodes']  # 5
datanode_size = data['datanode_size']  # 10 #10 blocks in 1 datanode
sync_period = data['sync_period']  # 180 #in miliseconds
namenode_checkpoints = data['namenode_checkpoints'].replace('$USER',getuser())
datanodes_path_list = []
#1234
#step1
#get input file, calculate size of file, calculate no. of blocks required
def compute_file_size():
    global file_size,number_of_blocks,file_name,given_input
    file_name=using_input#input("enter the file name along with extension")
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
        datanode_X=path_to_datanodes+str("/")+datanode_name
        fp=open(datanode_X,"w")
        datanodes_path_list.append([datanode_X,int(0)])
        fp.close()

#step3
#do the same for namenode
def create_namenode():
    global namenode_path
    namenode_path=str(path_to_namenodes+"/namenode.txt")
    fp=open(namenode_path,"a")
    fp.close()

#step4
#handle Namenode and call BLOCK within
def NAMENODE():

    fp_namenode=open(namenode_path,"w")
    fp_user_file=open(file_name,"r")

    word=""
    letter=""
    for i in range(1,number_of_blocks+1):
        flag=True
        while(flag):
            path_offset=random.choice(datanodes_path_list)#optimize
            path=path_offset[0]
            offset=path_offset[1]#check if data node is full, put of 1f(offset<=block_size*10)
            if(offset<=(block_size*datanode_size)-block_size):
                flag=False

        line=""
        checker=0
        for x in range(len(word),block_size-1):
            try:
                checker=0
                letter=fp_user_file.read(1)
                word=word+letter
                if(x==block_size-1):
                    letter1=fp_user_file.read(1)
                    if(letter==" " or letter1==" "):
                        line=line+word
                        word=""
                    else:
                        for y in range(block_size-1-len(line)):
                            line=line+"\x00"
                        word=word+letter1
                else:
                    if(letter==" " or letter=="\n"):
                        line=line+word
                        word=""
                        checker=1

            except:
                break
        if(letter=='' and checker==0):
            line=line+word
        global to_folder2    
        fp_namenode.write(str(to_folder2+'/'+file_name.split('/')[-1]) +" "+ "Block: " + str(i) + " " + str(path) + " " + "offset: " + str(offset) + " " + "rep_factor: 0" +" "+ "block_length: " +str(len(line))+"\n");
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
                    fp_namenode.write(str(to_folder2+'/'+file_name.split('/')[-1])+" "+"Block: "+str(i)+" "+str(path_new) +" "+ "offset: " + str(offset_new_x)+" "+ "rep_factor: "+str(rep)+ " " +"block_length: "+str(len(line))+ "\n")
                    offset_new_x=int(offset_new_x)+len(line)+1
                    datanodes_path_list[datanodes_path_list.index(path_offset_new)] = [path_new, offset_new_x]
                    rep+=1





    fp_namenode.close()
    fp_user_file.close()

def Mapper(line):  # Mapper
    global mymap
    return mymap.doo(line)
    #return mapper.doo(line)

def Reducer(item):  # Reducer
    global myred
    return(myred.doo(item))
    #return reducer.doo(item)

    
def Pass_To_Mapper(path,offset,end_offset):
  
  try:
      fhand = open(path, "r")
  except OSError:
    print("Could not open/read file:", path);


  fhand.read(int(offset))
  line=""
  for x in range(int(end_offset)):
    try:
     line=line+fhand.read(1)
    except:
      break
  fhand.seek(0);
  #Calling the class object which in turn invokes __call__ Function
 
  return line;


def BLOCK(path,line):
    fp_datanode_x=open(path,"a")
    fp_datanode_x.write(line+"\n")
    fp_datanode_x.close()

#call only if HDFS space is bigger than the file to be stored
#if __name__ == '__main__':
def yah(given_input,output,given_config,mapper,reducer):
    
    global using_input
    using_input=given_input
    config = given_config#"config/test_config.json"
    f = open(config)
    data = json.load(f)
    global block_size,path_to_datanodes,path_to_namenodes,replication_factor,num_datanodes,datanode_size,datanodes_path_list
    block_size = data['block_size']
    path_to_datanodes = data['path_to_datanodes'].replace('$USER',getuser())
    if not os.path.isdir(path_to_datanodes):
        os.mkdir(path_to_datanodes)
    path_to_namenodes = data['path_to_namenodes'].replace('$USER',getuser())
    if not os.path.isdir(path_to_namenodes):
        os.mkdir(path_to_namenodes)
    replication_factor=3
    num_datanodes = data['num_datanodes']  # 5
    datanode_size = data['datanode_size']  # 10 #10 blocks in 1 datanode
    sync_period = data['sync_period']  # 180 #in miliseconds
    namenode_checkpoints = data['namenode_checkpoints'].replace('$USER',getuser())
    fs_path=data["fs_path"].replace('$USER',getuser())
    datanodes_path_list = []
    vir=fs_path+'virtual_path.txt'
    if os.path.isfile(vir):
        temp=open(vir,'r')
        #print(temp)
        files=temp.readlines()
        temp.close()
        #print(files,given_input)
        if given_input+'\n' in files:
            
            # compute_file_size()
            # if(math.floor(num_datanodes*block_size*datanode_size/replication_factor)>=file_size):
            #     create_datanodes()
            #     create_namenode()
            #     NAMENODE()
            # else:
            #     print("File size too big for HDFS to compute")
            with open(path_to_namenodes+'namenode.txt', 'r') as fp:
                for countt, line in enumerate(fp):
                    pass
                #print(countt)
            global mymap,myred
            mymap=importlib.import_module(mapper)
            myred=importlib.import_module(reducer)    
            mappper = MapReduce(Mapper, Reducer)#returning an oject of the class to mappper
            Width = 6;#After removing irrelevent Widthords(Characters) the size of the Widthidth required
            Matrix = [[0 for x in range(Width)] for y in range(countt+1)]
            #print('going')
            try:
                file1 = open(path_to_namenodes+'namenode.txt', 'r')
            except OSError:
                print("Could not open NameNode ....Terminating");
                sys.exit("File not Open Error");

            Lines = file1.readlines();
            count = 0;
            for line in Lines:
                line = line.strip();
                line_array = line.split(' ');
                line_array.pop(1);
                line_array.pop(3);
                line_array.pop(4);
                line_array.pop(5);

                Matrix[count] = line_array;
                #print(line_array)
                count += 1;
            #print(Matrix)
            Block_Flag=[];
            #print(Block_Flag)
            Accessed_Blocks =[]
            input=[];
            #print(Matrix[0][0]);
            #global file_name
            #filename = file_name
            #print(countt)
            for i in range(countt):
                    #print(Matrix[i][0])
                #if(filename == Matrix[i][0]):
                #print(Matrix[i][0])
                    if(int(Matrix[i][1]) in Accessed_Blocks ):
                        continue;
                    elif((int(Matrix[i][1]) not in Accessed_Blocks)):
                        Accessed_Blocks.append(int(Matrix[i][1]));
                        
                        input.append(Pass_To_Mapper(Matrix[i][2],Matrix[i][3],Matrix[i][5]));
            #print(input)
            word_counts = mappper(input)
        #  if(len(Accessed_Blocks) < number_of_blocks):
        #      sys.exit("Block Missing even though replication is used,Reasons:-Block maybe missing (or) Block is corrupt (Or) Read write premissions are not given");
            #Calling the class object which in turn invokes __call__ Function
        
            f=open(output,'w')
            longest = max(len(word) for word, count in word_counts)#For having symmetrical space all through the output by selecting the longest word
            for word, count in word_counts:#Iterating through each tuple
                print('%-*s: %5s' % (longest+1, word, count));#The "%-*s" indicates a left trailing space whose value will be passes by longest+1 in the print statement
                f.write('%-*s: %5s\n' % (longest+1, word, count))
            f.close()  
        else:
            print(f"yah: error '{given_input}' not present in DFS.")          
#yah('test/sam.txt','output.txt','config/test_config.json','mapper','reducer')
   #mappper = MapReduce(Mapper, Reducer)#returning a class object to mapper

def put(from_file,to_folder,given_config):
    global using_input,to_folder2
    to_folder2=to_folder
    using_input=from_file
    config = given_config#"config/test_config.json"
    f = open(config)
    data = json.load(f)
    global block_size,path_to_datanodes,path_to_namenodes,replication_factor,num_datanodes,datanode_size,datanodes_path_list
    block_size = data['block_size']
    path_to_datanodes = data['path_to_datanodes'].replace('$USER',getuser())
    if not os.path.isdir(path_to_datanodes):
        os.mkdir(path_to_datanodes)
    path_to_namenodes = data['path_to_namenodes'].replace('$USER',getuser())
    if not os.path.isdir(path_to_namenodes):
        os.mkdir(path_to_namenodes)
    replication_factor=3
    num_datanodes = data['num_datanodes']  # 5
    datanode_size = data['datanode_size']  # 10 #10 blocks in 1 datanode
    sync_period = data['sync_period']  # 180 #in miliseconds
    namenode_checkpoints = data['namenode_checkpoints'].replace('$USER',getuser())
    fs_path=data['fs_path'].replace('$USER',getuser())
    datanodes_path_list = []
    compute_file_size()
    if(math.floor(num_datanodes*block_size*datanode_size/replication_factor)>=file_size):
        if not os.path.isdir(fs_path):
            l=fs_path.split('/')
            for i in range(2,len(l)):
                #print('/'+'/'.join(l[1:i]))
                if not os.path.isdir('/'+'/'.join(l[1:i])):
                #print('making')
                    os.mkdir('/'+'/'.join(l[1:i])) 
            #os.mkdir(fs_path)
        vir=fs_path+'/virtual_path.txt'
        if not os.path.isfile(vir):
            f=open(vir,'w')
            f.close()
        f=open(vir,'r')
        files=f.readlines()
        f.close()    
        f=open(vir,'w')
        if not (to_folder+'/'+from_file.split('/')[-1]+'\n') in files:
            f.write(to_folder+'/'+from_file.split('/')[-1]+'\n')
        else:
            print(f'put: error - file "{from_file}" already in DFS')    
        f.close() 
        create_datanodes()
        create_namenode()
        NAMENODE()
               
    else:
        print("File size too big for HDFS to compute")    
#put('input/crypto.txt','test1','config/test_config.json')           

