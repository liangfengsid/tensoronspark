import tensorspark.core.weight_combiner as comb
import numpy as np

def test_delta_weight_combiner():
	combiner = comb.DeltaWeightCombiner()
	origin = np.array([0, 0, 0])
	updated_1 = np.array([2, 2, 2])
	updated_2 = np.array([3, 3, 2])
	updated_3 = np.array([2, 4, 3])
	result1 = combiner.compute(origin, updated_1, worker_id='1', name='a')
	result2 = combiner.compute(result1, updated_2, worker_id='2', name='a')
	result3 = combiner.compute(result2, updated_3, worker_id='2', name='a')
	valid_result1 = np.array([2, 2, 2])
	valid_result2 = np.array([5, 5, 4])
	valid_result3 = np.array([2, 4, 3])
	assert(list(result1) == list(valid_result1))
	assert(list(result2) == list(valid_result2))
	assert(list(result3) == list(valid_result3))


#test_delta_weight_combiner()