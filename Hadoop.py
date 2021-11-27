import reducer
import mapper
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
import json
sys.path.insert(1, '/home/pes2ug19cs413/Desktop/dfs')
sys.path.insert(1, '/home/pes2ug19cs413/Desktop/dfs')
path = os.getcwd()


class MapReduce(object):

    # Activated when we create the class object
    def __init__(self, map_func, reduce_func, num_workers=None):
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
        # Assigning the number of processes(Concurrent execution) u want, Default is your CPU cores
        self.pool = multiprocessing.Pool(num_workers)

    def partition(self, mapped_values):
        """Organize the mapped values by their key.
        Returns an unsorted sequence of tuples with a key and a sequence of values.

        [('money', [1]), ('over', [1, 1]), ('centralized', [1]), ('ledger', [1, 1]), ('paper', [1]), ('through', [1]), ('validators', [1]), ('issuer', [1]), ('proportion', [1]), ('to', [1, 1, 1, 1, 1, 1, 1, 1]), ('issued', [1, 1]), ('first', [1, 1]), ('return', [1]), ('cryptography', [1]), ('get', [1, 1]), ('records', [1, 1]), ('they', [1, 1, 1]), ('not', [1, 1, 1]), ('using', [1]), ('bank', [1]), ('like', [1]), ('generally', [1, 1]), ('crypto', [1, 1, 1]), ('serves', [1]), ('token', [1, 1, 1]), ('stored', [1]), ('each', [1]), ('release', [1]), ('works', [1]), ('schemes', [1]), ('financial', [1]), ('creation', [1]), ('some', [1]), ('authority', [1, 1]), ('individual', [1]), ('are', [1, 1, 1]), ('implemented', [1]), ('stakers', [1]),....)]


        """
        partitioned_data = collections.defaultdict(
            list)  # Defaultdict returns a dictionary-like objectwhere a keyerror corresponds to a list in this case
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return(sorted(partitioned_data.items(), key=lambda kv: kv[0], reverse=False))
        # return partitioned_data.items()#items() method is used to return the list with all dictionary keys with values.

    def __call__(self, inputs, chunksize=1):  # Activated when we call the object of the class
        """Process the inputs through the map and reduce functions given.

        inputs
          An iterable containing the input data to be processed.

        chunksize=1
          The portion of the input data to hand to each worker.  This
          can be used to tune performance during the mapping phase.
        """
        map_responses = self.pool.map(
            self.map_func, inputs, chunksize=chunksize)  # Multithreading the mapper part
        # combine all and pass it to partition function
        partitioned_data = self.partition(itertools.chain(*map_responses))
        # Multithreading the reducer part
        reduced_values = self.pool.map(self.reduce_func, partitioned_data)
        return reduced_values


# 1234
f = open('test_config.json')
data = json.load(f)
# print(data)
block_size = data['block_size']  # in bytes for now
path_to_datanodes = data['path_to_datanodes']
# print("1",path_to_datanodes)#"/home/pes2ug19cs413/Desktop/DATANODES"
# "/home/pes2ug19cs413/Desktop/NAMENODES"
path_to_namenodes = data['path_to_namenodes']
replication_factor = data['replication_factor']  # 3
num_datanodes = data['num_datanodes']  # 5
datanode_size = data['datanode_size']  # 10 #10 blocks in 1 datanode
sync_period = data['sync_period']  # 180 #in miliseconds
# "/home/pes2ug19cs413/Desktop/NAMENODES/CHECKPOINTS"
namenode_checkpoints = data['namenode_checkpoints']
# 1234
# step1
# get input file, calculate size of file, calculate no. of blocks required


def compute_file_size():
    global file_size, file_name, number_of_blocks
    file_name = input("enter the file name along with extension - ")
    file_size = os.path.getsize(file_name)
    # print(file_size)
    number_of_blocks = math.ceil(file_size/(block_size-1))


# step2
# create X datanodes inside DATANODE directory
# X ranges from 1 to num_datanodes(pre defined variable)
def create_datanodes():
    # print(path_to_datanodes)
    # print(data['path_to_datanodes'])
    if not os.path.isdir(path_to_datanodes):
        os.mkdir(path_to_datanodes)
    global datanodes_path_list
    datanodes_path_list = []
    for i in range(1, num_datanodes+1):
        datanode_name = "datanode"+str(i)+".txt"
        datanode_X = path_to_datanodes+str("/")+datanode_name
        fp = open(datanode_X, "w")
        datanodes_path_list.append([datanode_X, int(0)])
        fp.close()

# step3
# do the same for namenode


def create_namenode():
    if not os.path.isdir(path_to_namenodes):
        os.mkdir(path_to_namenodes)
    global namenode_path
    namenode_path = str(path_to_namenodes+"/namenode.txt")
    fp = open(namenode_path, "w")
    fp.close()

# step4
# handle Namenode and call BLOCK within


def NAMENODE():

    fp_namenode = open(namenode_path, "w")
    fp_user_file = open(file_name, "r")
    # fp_namenode.write(file_name+"###"+str(number_of_blocks)+"\n") #Can't Use this and parse the string from the text file so i am commenting it out
    for i in range(1, number_of_blocks+1):
        flag = True
        while(flag):
            path_offset = random.choice(datanodes_path_list)  # optimize
            path = path_offset[0]
            # check if data node is full, put of 1f(offset<=block_size*10)
            offset = path_offset[1]
            if(offset <= (block_size*datanode_size)-block_size):
                flag = False
        fp_namenode.write("Block: "+str(i)+" "+str(path) +
                          " "+"offset: "+str(offset)+" "+"rep_factor: 0"+"\n")
        line = ""
        for x in range(block_size-1):
            try:
                line = line+fp_user_file.read(1)
            except:
                break
        offset_new = int(offset)+len(line)+1
        datanodes_path_list[datanodes_path_list.index(path_offset)] = [
            path, offset_new]
        BLOCK(path, line)

# replication
        lingo_list = []
        lingo_list.append(path_offset[0])
        rep = 1
        while(rep < replication_factor):
            path_offset_new = random.choice(datanodes_path_list)
            if(path_offset_new[0] not in lingo_list):
                lingo_list.append(path_offset_new[0])
                path_new = path_offset_new[0]
                offset_new_x = path_offset_new[1]
                if(offset_new_x <= (block_size*datanode_size)-block_size):
                    BLOCK(path_new, line)
                    fp_namenode.write("Block: "+str(i)+" "+str(path_new) + " " + "offset: " + str(
                        offset_new_x) + " " + "rep_factor: "+str(rep) + "\n")
                    offset_new_x = int(offset_new_x)+len(line)+1
                    datanodes_path_list[datanodes_path_list.index(path_offset_new)] = [
                        path_new, offset_new_x]
                    rep += 1

    fp_namenode.close()
    fp_user_file.close()


def BLOCK(path, line):
    fp_datanode_x = open(path, "a")
    fp_datanode_x.write(line+"\n")
    fp_datanode_x.close()


def Mapper(line):  # Mapper

    return mapper.doo(line)


def Reducer(item):  # Reducer

    return(reducer.doo(item))


def Pass_To_Mapper(path, offset):

    try:
        fhand = open(path, "r")
    except OSError:
        print("Could not open/read file:", path)

    fhand.read(int(offset))
    line = ""
    for x in range(block_size-1):
        try:
            line = line+fhand.read(1)
        except:
            break
    fhand.seek(0)
    # Calling the class object which in turn invokes __call__ Function

    return line


if __name__ == '__main__':  # Using This for better code understanding :-)
    # call only if HDFS space is bigger than the file to be stored
    #  block_size=300 #in bytes for now
    #  path_to_datanodes="/home/pes2ug19cs413/Desktop/DATANODES"
    #  path_to_namenodes="/home/pes2ug19cs413/Desktop/NAMENODES"
    #  replication_factor=3
    #  num_datanodes=20
    #  datanode_size=20 #10 blocks in 1 datanode
    #  sync_period=180 #in miliseconds
    #  namenode_checkpoints="/home/pes2ug19cs413/Desktop/NAMENODES/CHECKPOINTS"
    block_size = data['block_size']  # in bytes for now
    path_to_datanodes = data['path_to_datanodes']
    #  print("1",path_to_datanodes)#"/home/pes2ug19cs413/Desktop/DATANODES"
    # "/home/pes2ug19cs413/Desktop/NAMENODES"
    path_to_namenodes = data['path_to_namenodes']
    replication_factor = data['replication_factor']  # 3
    num_datanodes = data['num_datanodes']  # 5
    datanode_size = data['datanode_size']  # 10 #10 blocks in 1 datanode
    sync_period = data['sync_period']  # 180 #in miliseconds
    namenode_checkpoints = data['namenode_checkpoints']
    compute_file_size()
    if(math.floor(num_datanodes*block_size*datanode_size/replication_factor) >= file_size):
        create_datanodes()
        create_namenode()
        NAMENODE()
    else:
        print("File size too big for HDFS to compute")

    mappper = MapReduce(Mapper, Reducer)  # returning a class object to mapper
    # After removing irrelevent Widthords(Characters) the size of the Widthidth required
    Width = 4
    Matrix = [[0 for x in range(Width)] for y in range(
        number_of_blocks*replication_factor)]

    try:
        file1 = open(path_to_namenodes+'/namenode.txt', 'r')
    except OSError:
        print("Could not open NameNode ....Terminating")
        sys.exit("File not Open Error")

    Lines = file1.readlines()
    count = 0
    for line in Lines:
        line = line.strip()
        line_array = line.split(' ')
        line_array.pop(0)
        line_array.pop(2)
        line_array.pop(3)
        Matrix[count] = line_array
        count += 1
    # print(Matrix)
    Block_Flag = [0 for x in range(number_of_blocks)]
# print(Block_Flag)
    Accessed_Blocks = []
    input = []
# print(Matrix[0][0]);
    for i in range(number_of_blocks*replication_factor):
        # print(Matrix[i][0])
        if(int(Matrix[i][0]) in Accessed_Blocks):
            continue
        elif(int(Matrix[i][0]) not in Accessed_Blocks):
            Accessed_Blocks.append(int(Matrix[i][0]))
            Block_Flag[int(Matrix[i][0]) - 1] = 1
            input.append(Pass_To_Mapper(Matrix[i][1], Matrix[i][2]))
            word_counts = mappper(input)
    if(0 in Block_Flag):
        sys.exit("Block Missing even though replication is used,Reasons:-Block maybe missing (or) Block is corrupt (Or) Read write premissions are not given")
        # Calling the class object which in turn invokes __call__ Function

    # For having symmetrical space all through the output by selecting the longest word
    longest = max(len(word) for word, count in word_counts)
    out = open('output.txt', 'w')
    for word, count in word_counts:  # Iterating through each tuple
        # The "%-*s" indicates a left trailing space whose value will be passes by longest+1 in the print statement
        print('%-*s: %5s' % (longest+1, word, count))
        out.write('%-*s: %5s\n' % (longest+1, word, count))
    out.close()
    # mappper = MapReduce(Mapper, Reducer)#returning a class object to mapper
