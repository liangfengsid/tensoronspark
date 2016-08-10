class WeightCombiner(object):
	def __init__(self):
		pass


	def compute(self, origin_weight, new_weight, worker_id=-1):
		raise NotImplementedError('method not implemented')


	def get_status(self):
		raise NotImplementedError('method not implemented')


class MeanWeightCombiner(WeightCombiner):
	def __init__(self, num):	
		super(WeightCombiner, self).__init__()	
		self._num = num


	def compute(self, origin_weight, new_weight, worker_id=-1):
		updated_weight = ((self._num - 1) * origin_weight + new_weight) / self._num
		return updated_weight


class UpdateMeanWeightCombiner(WeightCombiner):
	def __init__(self, server):
		self._server = server


	def compute(self, origin_weight, new_weight, worker_id=-1):
		if self._server._version == 0:
			return new_weight
		else:
			return (origin_weight + new_weight) / 2


class DeltaWeightCombiner(WeightCombiner,):
	def __init__(self):
		super(WeightCombiner, self).__init__()
		self.last_weight = {}


	def compute(self, origin_weight, new_weight, worker_id=-1):
		# Param new_weight stands for the delta from the original weight
		if worker_id == -1:
			raise ValueError('Not recognized worker in DeltaWeightCombiner')
		try:
			weight = self.last_weight[worker_id]
			delta_weight = new_weight - weight
		except KeyError:
			delta_weight = new_weight
			
		updated_weight = origin_weight + delta_weight
		self.last_weight[worker_id] = new_weight
		return updated_weight


	def reset_value():
		self.last_weight = {}
