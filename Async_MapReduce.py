import collections
import itertools
import multiprocessing
import operator
import glob
import multiprocessing
import string
"""
This is a MapReduce program where an individual mapper thread works on an individual file in a dfs evnironment which is the goal for our assignment

YET ANOTHER HADOOP
"""
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
        #print('Partioned',partitioned_data.items());
        return partitioned_data.items()#items() method is used to return the list with all dictionary keys with values.
    
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

####

def Mapper(filename):#Mapper
    """Read a file and return a sequence of (word, occurances) values.
    Output for one file ex:-
    
     [('s', 1), ('newly', 1), ('minted', 1), ('tokens', 1), ('or', 1), ('other', 1), ('such', 1), ('reward', 1), ('mechanisms', 1), ('cryptocurrency', 1), ('does', 1), ('not', 1), ('exist', 1), ('in', 1), ('physical', 1), ('form', 1), ('like', 1), ('paper', 1), ('money', 1), ('and', 1), ('is', 1), ('typically', 1), ('not', 1), ('issued', 1), ('by', 1), ('a', 1), ('central', 1), ('authority', 1), ('cryptocurrencies', 1), ('typically', 1), ('use', 1), ('decentralized', 1), ('control', 1), ('as', 1), ('opposed', 1), ('to', 1), ('a', 1), ('central', 1), ('bank', 1), ('digital', 1), ('currency', 1), ('cbdc', 1), ('when', 1), ('a', 1), ('cryptocurrency', 1), ('is', 1), ('minted', 1), ('or', 1), ('created', 1), ('prior', 1), ('to', 1), ('issuance', 1), ('or', 1), ('issued', 1), ('by', 1), ('a', 1), ('single', 1), ('issuer', 1), ('it', 1), ('is', 1), ('generally', 1), ('consid', 1)])


    """
    
    TR = string.maketrans(string.punctuation, ' ' * len(string.punctuation))

    print multiprocessing.current_process().name, 'reading', filename
    output = []

    with open(filename, 'rt') as f:
        for line in f:
            
            line = line.translate(TR) # Strip punctuation
            line = line.translate(None, string.digits)# Strip digits
            for word in line.split():#Taking each individual word
                word = word.lower()#convert the word to lowercase
                output.append( (word, 1) )#Append to the list named output
    #print('Mapper output',output);
    return output


def Reducer(item):#Reducer
    """Convert the partitioned data for a word to a
    tuple containing the word and the number of occurances.
    
    Output Ex:-
    
    ('money', 1)
('over', 2)
('centralized', 1)
('ledger', 2)
('paper', 1)
('through', 1)
('validators', 1)
('issuer', 1)
('proportion', 1)
('to', 8)
('issued', 2)
('first', 2)
('return', 1)
('cryptography', 1)
('get', 2)
('records', 2)
('they', 3)
('not', 3)
('using', 1)
('bank', 1)
('like', 1)
('token', 3)

    """
    word, occurances = item
    #print(word, sum(occurances));
    return (word, sum(occurances))
    
    
input_files = glob.glob('*.txt')#This method is used to select all txt files in  a directory
mapper = MapReduce(Mapper, Reducer)#returning a class object to mapper
word_counts = mapper(input_files)#Calling the class object which in turn invokes __call__ Function
word_counts.sort()#Sorting before passing to
#word_counts.reverse()#Reversing for descending order
    
    
longest = max(len(word) for word, count in word_counts)#For having symmetrical space all through the output by selecting the longest word
for word, count in word_counts:#Iterating through each tuple
    print('%-*s: %5s' % (longest+1, word, count));#The "%-*s" indicates a left trailing space whose value will be passes by longest+1 in the print statement
    
