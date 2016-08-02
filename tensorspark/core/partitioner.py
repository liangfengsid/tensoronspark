import random

class RandomPartitioner(object):
	def __init__(self, num_partition):
		self.num_partition = num_partition
	def __eq__(self, other):
		return (isinstance(other, RandomPartitioner) and self.num_partition == other.num_partition)
	def __call__(self, k):
		return random.randint(0, self.num_partition - 1)