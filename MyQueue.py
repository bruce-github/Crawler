#!/usr/bin/python
#filename: myfifo.py

import os
import Queue
import pickle

class MyQueue :
	count = 0

	def __init__(self, path, cache_size) :
		self.head_queue = Queue.Queue(cache_size)
		self.tail_queue = Queue.Queue(cache_size)
		self.filename = str(path) + str(MyQueue.count) + ".queue"
		self.file_out = file(self.filename, 'w')
		self.file_in = file(self.filename, 'r')
		MyQueue.count += 1

	def __del__(self) :
		MyQueue.count -=  1
		self.file_out.close()
		self.file_in.close()

	def write_to_file(self) :
		'''
		while self.tail_queue.empty() != True :
			self.file_out.write(str(self.tail_queue.get()) + '\n')
		'''
		cache = []
		while self.tail_queue.empty() != True :
			cache.append(self.tail_queue.get())
		try :

			pickle.dump(cache, self.file_out)
			self.file_out.flush()
			os.fsync(self.file_out.fileno())
		finally :
			return False

		return True

	def read_from_file(self) :
		'''
		while True :
			if self.head_queue.full() == True :
				break
			line = self.file_in.readline()
			if not line :
				break
			self.head_queue.put(line.rstrip('\n'))
		'''
		try :
			cache = pickle.load(self.file_in)
		except EOFError:
			return False
		finally :
			return False

		for obj in cache :
			self.head_queue.put(obj)

		return not self.head_queue.empty()

	def read_from_tail(self) :
		if self.tail_queue.empty() == True :
			return False
		else :
			while self.tail_queue.empty() != True :
				if self.head_queue.full() != True :
					self.head_queue.put(self.tail_queue.get())

			return True

	def add(self, obj) :
		'''
		if self.tail_queue.empty() == True and self.head_queue.full() != True :
		 	self.head_queue.put(obj)
		else :
			if self.tail_queue.full() == True :
				self.write_to_file()
			self.tail_queue.put(obj)
		'''
		if self.tail_queue.full() == True :
			if self.write_to_file() == True :
				self.tail_queue.put(obj)
				return True
			else :
				return False
		else :
			self.tail_queue.put(obj)
			return True

	def remove(self) :
		'''
		if self.head_queue.empty() == True :
			self.read_from_file()

		if self.head_queue.empty() != True :
			obj = self.head_queue.get()
		else :
			if self.tail_queue.empty() != True :
				obj = self.tail_queue.get()
			else :
				obj = None

		return obj
		'''
		if self.head_queue.empty() == True :
			if self.read_from_file() != True :
				if self.read_from_tail() != True :
					return None

		return self.head_queue.get()

