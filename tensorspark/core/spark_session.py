import tensorflow as tf
from tensorflow.python.client.session import BaseSession
from tensorflow.python import pywrap_tensorflow as tf_session
from tensorflow.python.util import compat
import os

from session_worker import SessionWorker
from param_server import ParameterServer
from param_server import ParameterServerHandler

# class SparkSession(BaseSession):
class SparkSession(object):

	def __init__(self, spark_context, session, user='', name='', target='', server_host='localhost', server_port=10080, graph=None, config=None, sync_interval=3, batch_size=10000):
		"""
		sc: the spark context
		session: the default tensorflow session
		user: the user name of hdfs
		name: the name of the session
		server_host: to notify the spark executors the hostname of the parameter server
		server_port: to notify the spark executors the port of the parameter server
		sync_interval: the interval to synchronize the parameters between the workers and the parameter server
		batch_size: the batch size of the train input
		"""
		# super(SparkSession, self).__init__(target, graph, config=config)
		# self._context_managers = [self.graph.as_default(), self.as_default()]
		self._session = session
		self._sc = spark_context
		self._sessionRDD = None
		self._user = user
		self._name = name
		self._version = 0
		self._session_path = ''
		self._session_meta_path = ''
		self._fetch_name = None
		self._fetch_type = None
		#TODO
		self._param_server_host = server_host
		self._param_server_port = server_port
		self._sync_interval = sync_interval
		self._batch_size = batch_size

		self.param_server = None


	def __enter__(self):
		for context_manager in self._session._context_managers:
			context_manager.__enter__()
		return self


	def __exit__(self, exec_type, exec_value, exec_tb):
		"""Resets resource containers on `target`, and close all connected sessions.
		A resource container is distributed across all workers in the
		same cluster as `target`.  When a resource container on `target`
		is reset, resources associated with that container will be cleared.
		In particular, all Variables in the container will become undefined:
		they lose their values and shapes.
		NOTE:
		(i) reset() is currently only implemented for distributed sessions.
		(ii) Any sessions on the master named by `target` will be closed.
		If no resource containers are provided, all containers are reset.
		Args:
			target: The execution engine to connect to.
			containers: A list of resource container name strings, or `None` if all of
			all the containers are to be reset.
			config: (Optional.) Protocol buffer with configuration options.
		Raises:
			tf.errors.OpError: Or one of its subclasses if an error occurs while
			resetting containers.
		"""	
		if exec_type is errors.OpError:
			logging.error('Session closing due to OpError: %s', (exec_value,))

		for context_manager in reversed(self._session._context_managers):
			context_manager.__exit__(exec_type, exec_value, exec_tb)

		self._session.close()

	@staticmethod
	def reset(target, containers=None, config=None):

		if target is not None:
			target = compat.as_bytes(target)
		if containers is not None:
			containers = [compat.as_bytes(c) for c in containers]
		else:
			containers = []
		tf_session.TF_Reset(target, containers, config)

	"""
	To evaluate the fetches in SparkSession
	@Params
	fetches: the fetches for the tf.Session().run()
	feed_rdd: the input features and labels of the spark rdd type
	feed_name_list: the list the feed names that will be used to generate the tensors/operations as the keys of feed_dict
	param_list: the tensors that need to synchronize with the parameter server
	feed_dict: the feed_dict for the local run in tf.Session().run(). when @feed_rdd is provided, feed_dict should be None.
	weight_combiner: the WeightCombiner object to indicate how the parameter server combiner the parameters. Default to MeanWeightCombiner
	shuffle_within_partition: if True, the workers re-sort the data in the partition between epochs.
	options: the options for the tf.Session().run()
	run_metadata: the run_metadata for tf.Session.run()
	server_reusable: the boolean value to indicate whether the parameter server can be reused from the last run session, if the param_dict is the unchanged.
		For example, when the same rdd is trained multiple epochs with data if different orders. 
	"""
	def run(self, fetches, feed_rdd=None, feed_name_list=None, param_list = None, feed_dict=None, weight_combiner=None, shuffle_within_partition=False, options=None, run_metadata=None, server_reusable=False):
		if feed_rdd is None:
			if self._is_session_updated():
				return self._session.run(fetches, feed_dict, options, run_metadata)

		assert(feed_name_list is not None)
		assert(param_list is not None)

		#save the session to hdfs
		(self._session_path, self._session_meta_path) = self.broadcast_session(self._session, self._user, self._name)
		tmp_local_dir = self._get_tmp_dir()
		(host, port) = self._get_webhdfs_host_port()
		# graph_bc = self.broadcast_graph(self._sc, self._session)

		#TODO
		# Spark Broadcast the parameters
		if isinstance(fetches, list):
			self._fetch_name = [fetch.name for fetch in fetches]
			self._fetch_type = [str(type(fetch)).split('\'')[1] for fetch in fetches]
		elif isinstance(fetches, dict):
			self._fetch_name = {key:fetches[key].name for key in fetches}
			self._fetch_type = {key:str(type(fetches[key])).split('\'')[1] for key in fetches}
		else:
			self._fetch_name = fetches.name
			self._fetch_type = str(type(fetches)).split('\'')[1]

		#param_bc is the spark broadcast of the running attributes
		param_name_list = [tensor.name for tensor in param_list]
		param_dict = {
			'user': self._user,
			'name': self._name,
			'version': self._version,
			'session_path': self._session_path,
			'session_meta_path': self._session_meta_path,
			'fetch_name': self._fetch_name,
			'fetch_type': self._fetch_type, 
			'options': options,
			'run_metadata': run_metadata,
			'tmp_local_dir': tmp_local_dir, 
			'host': host, 
			'hdfs_port': port,
			'feed_name_list': feed_name_list,
			'param_name_list': param_name_list,
			# 'feed_type_list': feed_type_list,
			'batch_size': self._batch_size,
			'param_server_host': self._param_server_host,
			'param_server_port': self._param_server_port,
			'sync_interval': self._sync_interval,
			'shuffle_within_partition': shuffle_within_partition,
		}
		param_bc = self._sc.broadcast(param_dict)

		#TODO
		"""
		init and start the parameter server
		"""
		num_worker = feed_rdd.getNumPartitions()

		"""
		Update: the server can be inherited from the last run, if the param_dict are unchanged.
		"""
		if (not server_reusable) or (self.param_server is None):
			self.param_server = ParameterServer(sess=self._session, param_dict=param_dict, num_worker=num_worker, weight_combiner=weight_combiner, port=self._param_server_port, reusable=server_reusable)
			self.param_server.start()
		else:
			self.param_server.update_info(param_dict=param_dict, num_worker=num_worker, weight_combiner=weight_combiner, port=self._param_server_port, reusable=server_reusable)

		def _spark_run_fn(splitIndex, partition):
			worker = SessionWorker(index=splitIndex, param_bc=param_bc)
			# params = param_bc.value
			# _run_fn(splitIndex, partition, params)
			worker.run(splitIndex=splitIndex, partition=partition)
			worker.close()
			return [1]

		feed_rdd.mapPartitionsWithIndex(_spark_run_fn).count()

		if not server_reusable:
			self.stop_param_server()


	# def broadcast_graph(self, sc, session):
	# 	with session.as_default():
	# 		meta_proto = tf.train.export_meta_graph()
	# 		meta_bc = sc.broadcast(meta_proto)
	# 	return meta_bc
	def broadcast_session(self, session, user, filename, base_dir=''):
		import time
		timestamp = str(int(time.time()*1000))
		filename = 'session_' + self._name + '_' + timestamp
		spark_hdfsdir = self._sc._conf.get('spark.hdfs.dir')
		if spark_hdfsdir is None:
			spark_hdfsdir = '/'
		(save_path, meta_save_path) = self.save_session_hdfs(self._session, user, filename, spark_hdfsdir)
		#TODO return the session RDD
		return (save_path, meta_save_path)


	def save_session_hdfs(self, session, user, filename, base_dir=''):
		"""
		@Params:
		user: user of hdfs;
		filename: name of file that the session model is saved to.
		base_dir: the hdfs directory of the saveing session
		"""
		import hdfs_util as hdfs
		local_dir = self._get_tmp_dir()
		if local_dir == '':
			local_dir = '.'

		#save the graph_def proto
		# graph_proto_filename = filename + '.pb'
		# graph_local_path = local_dir + '/' + graph_proto_filename
		# graph_hdfs_path = base_dir + '/' + graph_proto_filename
		# tf.train.write_graph(session.graph_def, local_dir, graph_proto_filename, False)

		#put the local file to webhdfs via restful apis. 
		(host, port) = self._get_webhdfs_host_port()
		# hdfs.put(host, user, base_dir, graph_local_path, port)

		local_save_path = self._save_session_local(session, filename, local_dir=local_dir)
		meta_local_save_path = local_save_path+'.meta'
		#TODO
		hdfs_path = base_dir + '/' + filename
		meta_hdfs_path = hdfs_path+'.meta'

		hdfs.put(host, user, base_dir, local_save_path, port)
		hdfs.put(host, user, base_dir, meta_local_save_path, port)

		#remove the temporary local file
		#Bug to be fixed: the local can be deleted before the file to put into HDFS.
		#os.remove(local_save_path)
		#os.remove(meta_local_save_path)
		return (hdfs_path, meta_hdfs_path)


	def _save_session_local(self, sess, filename, local_dir=''):
		file_path = local_dir + '/' + filename
		with sess.as_default():
			saver = tf.train.Saver()
			save_path = saver.save(sess, file_path)
			return save_path

	#TODO
	def _get_tmp_dir(self):
		return '/tmp'

	def _get_webhdfs_host_port(self):
		defaultFS = self._sc._jsc.hadoopConfiguration().get('fs.defaultFS')
		host = 'localhost'
		if defaultFS is not None:
			host_port = ''
			sp = defaultFS.split('://')
			if len(sp) is 2:
				host_port = sp[1]
			else:
				host_port = sp[0]
			host = host_port.split(':')[0]


		port = str(50070)
		http_address = self._sc._jsc.hadoopConfiguration().get('dfs.namenode.http-address')
		if http_address is not None:
			sp = http_address.split(':')
			if len(sp) is 2:
				port = sp[1]

		return (host, port)


	#TODO
	def _is_session_updated(self):
		return True


	def get_session(self):
		return self._session


	def stop_param_server(self):
		if self.param_server is not None:
			self.param_server.stop()
			self.param_server = None






