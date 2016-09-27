import tornado.websocket as websocket
import tornado.web as web
from tornado.ioloop import IOLoop
import threading
import numpy as np
import json
import sets

import session_util as sutil
from weight_combiner import MeanWeightCombiner

class ParameterServer(threading.Thread):
	def __init__(self, sess, param_dict, num_worker, weight_combiner=None, port=10080, reusable=False):
		threading.Thread.__init__(self)
		self._session = sess
		self._port = port
		self._param_dict = param_dict
		self._application = web.Application([(r"/", ParameterServerHandler, {'server':self})])
		self._version = 0
		self._sync_lock = threading.Lock()
		self._num_worker = num_worker
		self._ended_worker = sets.Set()
		self._http_server = None
		self._reusable = reusable

		if weight_combiner is None:
			self._weight_combiner = MeanWeightCombiner(num_worker)
		else:
			self._weight_combiner = weight_combiner


	def run(self):
		self._http_server = self._application.listen(self._port)
		IOLoop.current().start()


	def stop(self):
		IOLoop.current().stop()
		if self._http_server is not None:
			self._http_server.stop()
			self._http_server = None


	def update_info(self, param_dict, num_worker, weight_combiner=None, port=10080, reusable=False):
		self._port = port
		self._param_dict = param_dict
		self._version = self._version + 1
		self._num_worker = num_worker
		self._ended_worker = sets.Set()
		self._reusable = reusable

		if weight_combiner is None:
			self._weight_combiner = MeanWeightCombiner(num_worker)
		else:
			self._weight_combiner = weight_combiner



class ParameterServerHandler(websocket.WebSocketHandler):
	def __init__(self, *args, **kwargs):
		self.server = kwargs.pop('server')
		self._session = self.server._session
		self._param_dict = self.server._param_dict
		self._lock = self.server._sync_lock
		super(ParameterServerHandler, self).__init__(*args, **kwargs)


	def open(self):
		pass


	def on_close(self):
		pass


	def on_message(self, msg):
		message = json.loads(msg)
		worker_id = message['worker_id']
		op = message['worker_op']
		if op == 'pull':
			param_name_list = self._param_dict['param_name_list']
			self._lock.acquire()
			local_parameters_ser = {name:sutil.get_tensor_value_by_name(self._session, name).tolist() for name in param_name_list}
			self._lock.release()
			param_meta = {'param_version': self.server._version, 'worker_id':worker_id, 'parameters': local_parameters_ser}
			self.write_message(param_meta)

		elif op == 'push':
			param_version = message['param_version']
			if param_version > self.server._version:
				raise ValueError('Parameter version from client is higher than that of the server.')
			parameters_msg = message['parameters']
			if parameters_msg is None:
				raise ValueError('Parameters from client is None')
				
			parameters = {key:np.array(parameters_msg[key]) for key in parameters_msg}
			self._lock.acquire()
			server_parameters = {name:sutil.get_tensor_value_by_name(self._session, name) for name in parameters}
			new_parameters = {}
			for name in parameters:
				num_worker = self.server._num_worker
				worker_value = parameters[name]
				server_value = server_parameters[name]
				new_value = self.server._weight_combiner.compute(server_value, worker_value, worker_id=worker_id, name=name)
				new_parameters[name] = np.array(new_value)
			sutil.apply_parameters(self._session, new_parameters)
			self.server._version += 1
			self._lock.release()

		elif op == 'end':
			if not self.server._reusable:
				self.server._ended_worker.add(worker_id)
				if len(self.server._ended_worker) == self.server._num_worker:
					self.server.stop()
					self._ended_worker = sets.Set()





