class WeightCombiner(object):
	def __init__(self):
		pass


	def compute(self, origin_weight, new_weight):
		raise NotImplementedError('method not implemented')


	def get_status(self):
		raise NotImplementedError('method not implemented')


class MeanWeightCombiner(WeightCombiner):
	def __init__(self, num):	
		super(WeightCombiner, self).__init__()	
		print('test')
		self._num = num


	def compute(self, origin_weight, new_weight):
		updated_weight = ((self._num - 1) * origin_weight + new_weight) / self._num
		return updated_weight


class UpdateMeanWeightCombiner(WeightCombiner):
	def __init__(self, server):
		self._server = server


	def compute(self, origin_weight, new_weight):
		if self._server._version == 0:
			return new_weight
		else:
			return (origin_weight + new_weight) / 2