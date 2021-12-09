import json
import time
import threading
from getpass import getuser
import sys

class NameNode(threading.Thread):

	def __init__(self, conf="./config/test_config.json",*args, **kwargs):
		
		super(NameNode, self).__init__(*args,**kwargs)
		self._stop = threading.Event()
		with open(conf) as f:
			data  = json.load(f)
			self.path = data['path_to_namenodes'].replace('$USER',getuser())+"namenode.txt"
			try:
				f = open(self.path,"r")
				f.close()
			except:
				f = open(self.path,"w")
				f.close()

	def stop(self):
		self._stop.set()

	def stopped(self):
		return self._stop.is_set()

	def put(self,nnblock):
		with open(self.path,'a') as f:
			for i in nnblock:
				f.write(i)
			print("writing to namenode...done.")
			
	def fetch(self, name):
		print(name)
		with open(self.path, 'r') as fp:
				for countt, line in enumerate(fp):
						pass
				print(countt)
		# mappper = MapReduce(Mapper, Reducer)#returning an oject of the class to mappper
		Width = 6;#After removing irrelevent Widthords(Characters) the size of the Widthidth required
		Matrix = [[0 for x in range(Width)] for y in range(countt+1)]
		
		try:
			file1 = open(self.path,'r')

		except OSError:
				#implement secondary namenode here
				print("Could not open NameNode ....Terminating");
				sys.exit("File not Open Error");

		Lines = file1.readlines();
		count = 0;
		for line in Lines:
				line = line.strip();
				line_array = line.split(' ');
				if line_array[0]==name:
					line_array.pop(1);
					line_array.pop(3);
					line_array.pop(4);
					line_array.pop(5);

					Matrix[count] = line_array;
					count += 1;
		# print(Matrix)
		return Matrix

				

	def cat(self,name):
		with open(self.path,"r") as f:
			# print("lol")
			for i in f.readlines():
				s = i.split(' ')
				print
				if s[0] != name or int(s[7]) != 0: continue
				try:
					f2 = open(s[3],"r")
					# pos = int(s[-1][0])
					pos = int(s[5])
					# print(pos)
					f2.seek(pos)
					print(f2.readline().rstrip('\n'))
					f2.close()
				except Exception as e: print(e)
			print()
		pass

	def run(self):
		count = 0
		print(self.path)
			#initialize
		with open(self.path) as f:
			while True:
				time.sleep(5)
		

# t1 = NameNode(daemon = True)
# t1.start()

# answer = input()
