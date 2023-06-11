import socket 
import threading
import os
import time
import hashlib
import json
import time


class Node:
	def __init__(self, host, port):
		self.stop = False
		self.host = host
		self.port = port
		self.M = 16
		self.N = 2**self.M
		self.key = self.hasher(host+str(port))

		threading.Thread(target = self.listener).start()
		self.files = []
		self.backUpFiles = [] #backup files are stored at the successor, because when a node leaves or fails, its hash ring area
								#now belong to its successor, so storing backup files at the successor prevents extra transfer of files
		if not os.path.exists(host+"_"+str(port)):
			os.mkdir(host+"_"+str(port))

		self.successor = (self.host, self.port)
		self.predecessor = (self.host, self.port)
		# additional state variables
		self.successor_successor = None  #the successor of the successor
		self.location = None   #the location of the node to which i have to send the file



	def Ping(self):
		failure_check = 0
		while True:
			soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			if self.successor!=None:
				try:
					soc.connect(self.successor)
					soc.sendto(("Ping " + json.dumps((self.host , self.port))).encode(), (self.successor[0], self.successor[1]))
				except:
					failure_check = failure_check + 1
					if failure_check==3:
						self.successor = self.successor_successor
						soc.connect(self.successor)
						soc.sendto(("UpdateLeaving " + json.dumps((self.host, self.port))).encode(), 
							(self.successor[0], self.successor[1]))

						time.sleep(0.5)

						for f in self.files:
							soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
							soc.connect(self.successor)
							soc.sendto(("AddBackupFile " + f + " " + json.dumps((self.host, self.port))).encode() ,
							    (self.successor[0], self.successor[1]))
							file_path = str(self.host) + "_" + str(self.port) + "/" + f

							soc.recv(1024).decode()

							self.sendFile(soc, file_path)

			time.sleep(0.5)


	def hasher(self, key):
		'''
		Use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N


	def handleConnection(self, client, addr):

		data = client.recv(4096)
		data_split = data.decode().split(" ", 1)
		new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		
		if data_split[0] == "Lookup":
			data_new_split = data_split[1].split(" ", 1)
			successor_addr = self.Lookup(int(data_new_split[0]))

			if successor_addr is None:
				new_sock.connect(self.successor)
				new_sock.sendto(("Lookup " + data_new_split[0] + " " + data_new_split[1]).encode() , 
				    (self.successor[0] , self.successor[1]))

			else:
				address = json.loads(data_new_split[1])
				new_address = tuple(address)
				try:
					new_sock.connect(new_address)
					new_sock.sendto(("Found " + json.dumps(successor_addr)).encode() , 
						    (new_address[0] , new_address[1]))
				except:
					pass


		elif data_split[0] == "Found":
			found = json.loads(data_split[1])
			self.successor = tuple(found)


		elif data_split[0] == "Update":
			recvd = json.loads(data_split[1])
			old_port = self.predecessor[1]
			old_predecessor = self.predecessor
			self.predecessor = tuple(recvd)
			to_remove = []

			if old_port != self.port: #to check if it is the result of pinging after join so it should not send backupfiles
				if len(self.backUpFiles) != 0:
					for f in self.backUpFiles:
						new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
						new_sock.connect(self.predecessor)
						new_sock.sendto(("AddBackupFile " + f + " " + json.dumps((self.host, self.port))).encode() ,
							(self.predecessor[0], self.predecessor[1]))

						new_sock.recv(1024).decode()

						file_path = str(self.host) + "_" + str(self.port) + "/" + f
						self.sendFile(new_sock, file_path)

						to_remove.append(f)
						os.remove(file_path)

					for r in to_remove:
						self.backUpFiles.remove(r)


				to_remove.clear()

				if len(self.files) != 0:
					for f in self.files:
						if self.hasher(f) <= self.hasher(self.predecessor[0]+str(self.predecessor[1])):
							if self.hasher(f) > self.hasher(old_predecessor[0]+str(old_predecessor[1])):
								new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
								new_sock.connect(self.predecessor)
								new_sock.sendto(("AddNewFile " + f + " " + json.dumps((self.host, self.port))).encode() ,
									(self.predecessor[0], self.predecessor[1]))

								new_sock.recv(1024).decode()

								file_path = str(self.host) + "_" + str(self.port) + "/" + f	 
								self.sendFile(new_sock, file_path)

								to_remove.append(f)

								new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
								new_sock.connect(self.successor)
								new_sock.sendto(("RemoveBackupFile " + f).encode(), (self.successor[0], self.successor[1]))


						elif self.hasher(f) >= self.hasher(self.predecessor[0]+str(self.predecessor[1])):
							if self.hasher(self.predecessor[0]+str(self.predecessor[1])) < self.hasher(old_predecessor[0]+str(old_predecessor[1])):	
								if self.hasher(f) > self.hasher(old_predecessor[0]+str(old_predecessor[1])):
									new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
									new_sock.connect(self.predecessor)
									new_sock.sendto(("AddNewFile " + f + " " + json.dumps((self.host, self.port))).encode() ,
										(self.predecessor[0], self.predecessor[1]))

									new_sock.recv(1024).decode()

									file_path = str(self.host) + "_" + str(self.port) + "/" + f	 
									self.sendFile(new_sock, file_path)

									to_remove.append(f)

									new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
									new_sock.connect(self.successor)
									new_sock.sendto(("RemoveBackupFile " + f).encode(), (self.successor[0], self.successor[1]))
									
					for r in to_remove:
						self.files.remove(r)
						self.backUpFiles.append(r)

					to_remove.clear()


		elif data_split[0] == "Ping":
			recvd = json.loads(data_split[1])
			address = tuple(recvd)
			try:
				new_sock.connect(address)
				new_sock.sendto(("Predecessor " + json.dumps(self.predecessor) + "  " + json.dumps(self.successor)).encode(),
				 (address[0], address[1]))
			except:
				pass


		elif data_split[0] == "Predecessor":
			data_new_split = data_split[1].split("  ", 1)
			recvd = json.loads(data_new_split[0])
			pred = tuple(recvd)

			if pred[1] == self.port:
				pass

			else:
				self.successor = pred
				new_sock.connect(self.successor)
				new_sock.sendto(("Update " + json.dumps((self.host, self.port))).encode() , (self.successor[0] , self.successor[1]))


			recd = json.loads(data_new_split[1])
			succ = tuple(recd)
			if succ == self.successor_successor:
				pass
			else:
				self.successor_successor = succ



		elif data_split[0] == "LookupFile":
			data_new_split = data_split[1].split(" ", 1)
			successor_addr = self.Lookup(int(data_new_split[0]))

			if successor_addr is None:
				new_sock.connect(self.successor)
				new_sock.sendto(("LookupFile " + data_new_split[0] + " " + data_new_split[1]).encode() , 
				    (self.successor[0] , self.successor[1]))

			else:
				address = json.loads(data_new_split[1])
				new_address = tuple(address)
				new_sock.connect(new_address)
				new_sock.sendto(("FoundFile " + json.dumps(successor_addr)).encode() , 
					    (new_address[0] , new_address[1]))


		elif data_split[0] == "FoundFile":
			found = json.loads(data_split[1])
			self.location = tuple(found)


		elif data_split[0] == "AddFile": # to add any new file
			data_new_split = data_split[1].split(" ", 1)
			self.files.append(data_new_split[0])
			file_path = str(self.host) + "_" + str(self.port) + "/" + data_new_split[0]
			client.send(("ok").encode())

			self.recieveFile(client, file_path)

			new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			new_sock.connect(self.successor)
			new_sock.sendto(("AddBackupFile " + data_new_split[0] + " " + json.dumps((self.host, self.port))).encode() ,
				(self.successor[0], self.successor[1]))

			new_sock.recv(1024).decode()
			self.sendFile(new_sock, file_path) 


		elif data_split[0] == "AddBackupFile": # to add any backup file
			data_new_split = data_split[1].split(" ", 1)
			self.backUpFiles.append(data_new_split[0])
			file_path = str(self.host) + "_" + str(self.port) + "/" + data_new_split[0]
			client.send(("ok").encode())

			self.recieveFile(client, file_path) 


		elif data_split[0] == "SendFile":  # to send file for get
			data_new_split = data_split[1].split(" ", 1)
			file_path = str(self.host) + "_" + str(self.port) + "/" + data_new_split[0]

			if data_new_split[0] in self.files:
				client.send(("ok").encode())
				self.sendFile(client, file_path)

			else:
				client.send(("no file").encode())


		elif data_split[0] == "AddNewFile": #to add file from update
			data_new_split = data_split[1].split(" ", 1)
			self.files.append(data_new_split[0])
			file_path = str(self.host) + "_" + str(self.port) + "/" + data_new_split[0]
			client.send(("ok").encode())

			self.recieveFile(client, file_path)


		elif data_split[0] == "RemoveBackupFile":
			self.backUpFiles.remove(data_split[1])
			file_path = str(self.host) + "_" + str(self.port) + "/" + data_split[1]
			os.remove(file_path)



		elif data_split[0] == "Leaving":
			recvd = json.loads(data_split[1])
			new_successor = tuple(recvd)
			self.successor = new_successor

			new_sock.connect(self.successor)
			new_sock.sendto(("UpdateLeaving " + json.dumps((self.host, self.port))).encode() , 
					    (self.successor[0] , self.successor[1]))


		elif data_split[0] == "UpdateLeaving":
			recvd = json.loads(data_split[1])
			self.predecessor = tuple(recvd)


			to_remove = []
			for f in self.backUpFiles:
				self.files.append(f)
				to_remove.append(f)

				new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				new_sock.connect(self.successor)
				new_sock.sendto(("AddBackupFile " + f + " " + json.dumps((self.host, self.port))).encode() ,
					(self.successor[0], self.successor[1]))

				new_sock.recv(1024).decode()

				file_path = str(self.host) + "_" + str(self.port) + "/" + f
				self.sendFile(new_sock, file_path)


			for r in to_remove:
				self.backUpFiles.remove(r)

			to_remove.clear()




		'''
		 Function to handle each inbound connection, called as a thread from the listener.
		'''

	def listener(self):
		'''
		Stock function. Not to edit
		'''
		listener = socket.socket()
		listener.bind((self.host, self.port))
		listener.listen(10)
		while not self.stop:
			client, addr = listener.accept()
			threading.Thread(target = self.handleConnection, args = (client, addr)).start()
		print ("Shutting down node:", self.host, self.port)
		try:
			listener.shutdown(2)
			listener.close()
		except:
			listener.close()
	


	def Lookup(self, find_key):
		if self.successor[1] == self.port: # to check if one node present
			return self.successor

		elif self.key<find_key:
			if self.hasher(self.successor[0]+str(self.successor[1])) < find_key: 
				if self.hasher(self.successor[0]+str(self.successor[1])) < self.key: #if successor is not larger key
					return self.successor

			else:
				return self.successor

		elif self.key > find_key:
			if self.hasher(self.successor[0]+str(self.successor[1])) > find_key:
				if self.hasher(self.successor[0]+str(self.successor[1])) < self.key:
					return self.successor

		else:
			pass



	def join(self, joiningAddr):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		if len(joiningAddr)>0:
			sock.connect(joiningAddr)
			sock.sendto(("Lookup " + str(self.key) + " " + json.dumps((self.host , self.port))).encode() , 
				(joiningAddr[0] , joiningAddr[1]))

			time.sleep(0.5)
			
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect(self.successor)


			sock.sendto(("Update " + json.dumps((self.host, self.port))).encode() , 
					    (self.successor[0] , self.successor[1]))
			

		threading.Thread(target = self.Ping).start()

		'''
		This function handles the logic of a node joining. This function should do a lot of things such as:
		Update successor, predecessor, getting files, back up files. .
		'''



	def put(self, fileName):
		soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		file_key = self.hasher(fileName)
		soc.connect(self.successor)
		soc.sendto(("LookupFile " + str(file_key) + " " + json.dumps((self.host , self.port))). encode(),
			(self.successor[0] , self.successor[1]))

		time.sleep(0.5)
		soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		soc.connect(self.location)
		soc.sendto(("AddFile " + fileName + " " + json.dumps((self.host, self.port))).encode() ,
		    (self.location[0], self.location[1]))

		soc.recv(1024).decode()

		self.sendFile(soc, fileName)


		'''
		This function should first find node responsible for the file given by fileName, then send the file over the socket to that node
		Responsible node should then replicate the file on appropriate node.Responsible node should save the files
		in directory given by host_port e.g. "localhost_20007/file.py".
		'''


		
	def get(self, fileName):
		soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		file_key = self.hasher(fileName)
		soc.connect(self.successor)
		soc.sendto(("LookupFile " + str(file_key) + " " + json.dumps((self.host , self.port))). encode(),
			(self.successor[0] , self.successor[1]))

		time.sleep(0.5)

		soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		soc.connect(self.location)
		soc.sendto(("SendFile " + fileName + " " + json.dumps((self.host, self.port))).encode(), 
		      (self.location[0], self.location[1]))


		recvd = soc.recv(1024).decode()

		if recvd == "no file":
			return None

		else:
			self.recieveFile(soc, fileName)
			return fileName


		'''
		This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
		i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.
		'''


		
	def leave(self):
		temporary_successor = self.successor
		self.successor = None

		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect(self.predecessor)
		sock.sendto(("Leaving " + json.dumps(temporary_successor)).encode() , (self.predecessor[0], self.predecessor[1]))

		time.sleep(1)

		for f in self.backUpFiles:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect(temporary_successor)
			sock.sendto(("AddBackupFile " + f + " " + json.dumps((self.host, self.port))).encode() ,
			    (temporary_successor[0], temporary_successor[1]))
			file_path = str(self.host) + "_" + str(self.port) + "/" + f

			sock.recv(1024).decode()

			self.sendFile(sock, file_path)



		self.predecessor = None
		self.stop = True

		'''
		When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
		it should send its share of file to the new responsible node, close all the threads and leave. Close listener thread
		by setting self.stop flag to True
		'''


	def sendFile(self, soc, fileName):
		print("send")
		''' 
		Utility function to send a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = os.path.getsize(fileName)
		soc.send(str(fileSize).encode('utf-8'))
		y = soc.recv(1024).decode('utf-8')
		with open(fileName, "rb") as file:
			contentChunk = file.read(1024)
			while contentChunk!="".encode('utf-8'):
				soc.send(contentChunk)
				contentChunk = file.read(1024)


	def recieveFile(self, soc, fileName):
		'''
		Utility function to recieve a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = int(soc.recv(1024).decode('utf-8'))
		soc.send("ok".encode('utf-8'))
		contentRecieved = 0
		file = open(fileName, "wb")
		while contentRecieved < fileSize:
			contentChunk = soc.recv(1024)
			contentRecieved += len(contentChunk)
			file.write(contentChunk)
		file.close()
		print("recv")

	def kill(self):
		self.stop = True

		
