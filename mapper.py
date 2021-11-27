from __future__ import print_function
import collections
import itertools
import operator
import glob
import multiprocessing
import string
import sys
import re
def doo(line):
 
 #print(multiprocessing.current_process().name, 'reading');
 #TR = string.maketrans(string.punctuation, ' ' * len(string.punctuation))

 #print(multiprocessing.current_process().name, 'reading', line)
 output = []


            
 res = re.sub(r'[^\w\s]','', line)# Strip punctuation
#line = line.translate(None, string.digits)# Strip digits
 for word in line.split():#Taking each individual word
  word = word.lower()#convert the word to lowercase
            #print(list(word, 1))
            #output.append(list((word, 1)))
  output.append((word,1));
 return output;

