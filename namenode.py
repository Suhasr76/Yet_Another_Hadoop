import json
import time
import threading
from getpass import getuser

class NameNode(threading.Thread):
	def __init__(self, config="./config/test_config.json",*args, **kwargs):
		super(NameNode, self).__init__(*args,**kwargs)
		self._stop = threading.Event()
		with open(config) as f:
			data  = json.load(f)
			self.path = data['path_to_namenodes'].replace('$USER',getuser())+"namenode.txt"

	def stop(self):
		self._stop.set()

	def stopped(self):
		return self._stop.is_set()

	def put(self,nnblock):
		with open(self.path,"w") as f:
			for i in nnblock:
				f.write(i)
			# print("writing to namenode...done.")

	def cat(self,name):
		with open(self.path,"r") as f:
			print("lol")
			for i in f.readlines():
				s = i.split(' ')
				if s[0] != name: continue
				try:
					f2 = open(s[3],"r")
					# pos = int(s[-1][0])
					pos = int(s[-3])
					# print(pos)
					f2.seek(pos)
					print(f2.readline().rstrip('\n'))
					f2.close
				except Exception as e: print(e)
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
