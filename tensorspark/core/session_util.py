import tensorflow as tf
from tensorflow.python.platform import gfile

# @staticmethod
def extract_fetch(sess, name, type=''):
	"""
	restore the fetch object by the @Param
	name: name of the fetch
	type: (optional) a string of the type name. may be Tensor, Operation, Sparse Tensor or Tensor Handle list
	"""
	with sess.graph.as_default():
		if type == 'tensorflow.python.framework.ops.Tensor':
			fetch = sess.graph.get_tensor_by_name(name)
		elif type == 'tensorflow.python.framework.ops.Operation':
			fetch = sess.graph.get_operation_by_name(name)
		else:
			#TODO
			#not sure for the sparse tensor type or when fetch is produced by get_tensor_handle op
			fetch = sess.graph.as_graph_element(name)	

	return fetch


def extract_variable(sess, name):
	with sess.graph.as_default():
		variables =  tf.all_variables()
		variable = [x for x in variables if x.name == name]
		if len(variable) == 0:
			return None
		else: 
			return variable[0]


# @staticmethod
def get_tensor_value_by_name(sess, name):
	tensor = extract_fetch(sess=sess, name=name)
	return tensor.eval(session=sess)


# @staticmethod
def apply_parameters(sess, params):
	for name in params:
		variable = extract_variable(sess, name)
		if variable is None:
			raise TypeError('Parameter is None')
		assign_op = variable.assign(params[name])
		sess.run(assign_op)


# @staticmethod
def restore_session_hdfs(sess, user, hdfs_path, meta_hdfs_path, tmp_local_dir, host, port):
	#download hdfs file
	# local_dir = self._get_tmp_dir()
	local_dir = tmp_local_dir
	filename = hdfs_path.split('/')[-1]
	meta_filename = meta_hdfs_path.split('/')[-1]

	import hdfs_util as hdfs
	(local_meta_path, local_path) = hdfs.get(host, user, [meta_hdfs_path, hdfs_path], local_dir)
	
	retry_time = 0
	import time
	import os
	while True:
		if os.path.exists(local_meta_path):
			break
		else:
			retry_time = retry_time + 1
			if retry_time > 10:
				raise OSError("Timeout for downloading file %s from HDFS" % local_meta_path)
			time.sleep(0.1)

	# sess_graph = hdfs.read(host, user, meta_hdfs_path, port)
	# meta_file = open(local_path, 'wb')
	# meta_file.write(sess_graph)
	# meta_file.close()
	
	# sess_text = hdfs.read(host, user, hdfs_path, port)
	# with open(local_path, 'wb') as file:
	# 	file.write(sess_text)
	# local_path = hdfs.get(host, user, hdfs_path, local_dir)

	with sess.graph.as_default():
		# with gfile.FastGFile(local_meta_path, 'rb') as f:
			# sess.graph_def.ParseFromString(f.read())
		# g = tf.GraphDef()
		# with gfile.FastGFile(local_meta_path, 'rb') as f:
		# 	# sess.graph_def.ParseFromString(sess_graph)
		# 	g.ParseFromString(f.read())
		# tf.import_graph_def(g)
		# saver = tf.train.Saver(tf.all_variables())
		# saver.restore(sess, local_path)
		saver = tf.train.import_meta_graph(local_meta_path)
		saver.restore(sess, local_path)

		# import os
		# os.remove(local_path)
		# os.remove(local_meta_path)

	