import tensorflow as tf
import tornado.websocket as websocket
from tornado.ioloop import IOLoop
from tornado import gen
import numpy as np
import json
import itertools

import session_util as sutil

class SessionWorker(object):
	def __init__(self, index, param_bc):
		self._id = index
		self._sess = None
		self._param_bc = param_bc
		self._last_version = -1

		self._websock = None
		self._ioloop = IOLoop.instance()
		self._ioloop.run_sync(self._init_websock)


	@gen.coroutine
	def _init_websock(self):
		#create the tornado websocket client
		params = self._param_bc.value
		param_server_host = params['param_server_host']
		param_server_port = params['param_server_port']
		self._websock = yield websocket.websocket_connect('ws://%s:%d' % (param_server_host, param_server_port), connect_timeout=3600)


	def run(self, splitIndex, partition):
		self._run_fn(splitIndex, partition, self._param_bc.value)


	def close(self):
		if self._websock is not None:
			self._websock.close()


	def _run_fn(self, splitIndex, partition, params):
		user = params['user']
		name = params['name']
		version = params['version']
		session_path = params['session_path']
		session_meta_path = params['session_meta_path']
		fetch_name = params['fetch_name']
		fetch_type = params['fetch_type']
		options = params['options']
		run_metadata = params['run_metadata']
		tmp_local_dir = params['tmp_local_dir']
		host = params['host']
		port = params['hdfs_port']
		feed_name_list = params['feed_name_list']
		param_name_list = params['param_name_list']
		# feed_type_list = params['feed_type_list']
		batch_size = params['batch_size']
		param_server_host = params['param_server_host']
		param_server_port = params['param_server_port']
		sync_interval = params['sync_interval']
		shuffle_within_partition = params['shuffle_within_partition']

		sess =  tf.Session()
		self._sess = sess
		with sess.as_default():
			print ('Worker ' + str(self._id) +' starts running')
			sutil.restore_session_hdfs(sess, user, session_path, session_meta_path, tmp_local_dir, host, port)
			fetches = None
			if isinstance(fetch_name, list):
				fetches = [sutil.extract_fetch(sess, fetch_name[i], fetch_type[i]) for i in range(len(fetch_name))]
			elif isinstance(fetch_name, dict):
				fetches = {key:sutil.extract_fetch(sess, fetch_name[key], fetch_type[key]) for key in fetch_name}
			else:
				fetches = sutil.extract_fetch(sess, fetch_name, fetch_type)

			feed_list = [sutil.extract_fetch(sess, name) for name in feed_name_list]

			length = len(feed_list)
			time = 0
			cursor = 0
			last_push_time = -1

			if shuffle_within_partition:
				if isinstance(partition, itertools.chain):
					partition = list(partition)

				if isinstance(partition, list):
					import random
					random.shuffle(partition)
				else:
					raise TypeError('The partition is not of the list type')

			while True:
				items = [[] for i in range(length)]
				for i in xrange(batch_size):
					"""
					To transform partition data from the iterator of tuples 
					to seperate lists of each feed items. E.g., 
					[(feature, label)] => [ [features], [labels]]
					"""
					try:
						if isinstance(partition, list):
							item = partition[cursor]
						else: 
							item = partition.next()
						assert(len(item) == length)
						for j in range(length):
							items[j].append(item[j])
					except (IndexError, StopIteration):
						break
					cursor += 1

				if len(items[0]) == 0:
					break

				if SessionWorker.time_to_sync(time=time, interval=sync_interval):
					self.pull_parameters()

				feed_dict = {feed_list[i]:items[i] for i in range(length)}
				sess.run(fetches, feed_dict, options, run_metadata)

				time += 1
				if SessionWorker.time_to_sync(time=time, interval=sync_interval):
					local_parameters = {name:sutil.get_tensor_value_by_name(sess, name).tolist() for name in param_name_list}
					param_meta = {'worker_id':self._id, 'worker_op':'push', 'param_version':self._last_version, 'parameters':local_parameters}
					self.push_parameters(param_meta)
					last_push_time = time

			#push the parameter updated by the remaining input items
			if time > last_push_time:
				local_parameters = {name:sutil.get_tensor_value_by_name(sess, name).tolist() for name in param_name_list}
				param_meta = {'worker_id':self._id, 'worker_op':'push', 'param_version':self._last_version, 'parameters':local_parameters}
				self.push_parameters(param_meta)

			self.notify_end()
			print ('Worker ' + str(self._id) +' stops running')



	@staticmethod
	def time_to_sync(time, interval):
		if int(time) % int(interval) ==0:
			return True
		else:
			return False


	def push_parameters(self, message):
		@gen.coroutine
		def _push_parameters_sync():
			if self._websock is None:
				raise TypeError('The websock is not initialized')
			self._websock.write_message(json.dumps(message))

		self._ioloop.run_sync(_push_parameters_sync)


	def pull_parameters(self):
		@gen.coroutine
		def _pull_parameters_sync():
			if self._websock is None:
				raise TypeError('The websock is not initialized')
			pull_request = {'worker_id':self._id, 'worker_op':'pull'}
			self._websock.write_message(json.dumps(pull_request))
			param_meta_msg = yield self._websock.read_message()
			# self._ioloop.add_future(param_meta_future, apply_parameters)
			param_meta = json.loads(param_meta_msg)
			return_id = param_meta['worker_id']
			if return_id != self._id:
				raise ValueError('The returned ID does not match the current worker ID')
			param_version = param_meta['param_version']
			if param_version <= self._last_version:
				return
			parameters = param_meta['parameters']
			if parameters is None:
				raise ValueError('Parameters from server is None')
			# deserialized = {key:np.loads(parameters[key]) for key in parameters}
			# sutil.apply_parameters(self._sess, deserialized)
			np_parameters = {key:np.array(parameters[key]) for key in parameters}
			sutil.apply_parameters(self._sess, np_parameters)
			self._last_version = param_version

		self._ioloop.run_sync(_pull_parameters_sync)


	def notify_end(self):
		end_msg = {'worker_id':self._id, 'worker_op':'end'}

		@gen.coroutine
		def _notify_end_sync():
			if self._websock is None:
				raise TypeError('The websock is not initialized')
			self._websock.write_message(json.dumps(end_msg))

		self._ioloop.run_sync(_notify_end_sync)


		






