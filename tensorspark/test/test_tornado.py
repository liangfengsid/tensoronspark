import tornado.web as web
from tornado.ioloop import IOLoop
import time
import threading

class Handler(web.RequestHandler):
	def get(self):
		self.write('Hello World')


class Server(threading.Thread):
	def __init__(self, port=11080):
		threading.Thread.__init__(self)
		self.app = web.Application([(r'/', Handler)])

		self.ioloop = IOLoop.current()
		self.ioloop.make_current()
		self.port = port
		self.server = None

	def run(self):
		# try:
		self.server = self.app.listen(self.port)
		self.ioloop.start()
		# except Exception as e:
			# print(e)
			# self.stop()

	def stop(self):
		self.ioloop.stop()
		self.server.stop()


def test_ioloop_instance():
	server1 = Server(11080)
	server2 = Server(12080)

	ioloop1 = server1.ioloop
	ioloop2 = server2.ioloop
	assert(ioloop1 == ioloop2)

	server1.start()
	server2.start()

	time.sleep(20)
	ioloop1.stop()
	#Test in brower whether localhost:12080 is still accessable. 

	server1.stop()
	server2.stop()


def test_server_stop():
	server = Server()
	server.start()
	time.sleep(5)
	server.server.stop()



def test_ioloop_stop():
	server = Server()
	server.start()
	print('Server started')
	ioloop = server.ioloop
	time.sleep(5)

	server.stop()
	print('Server stopped')
	time.sleep(5)

	server = Server()
	server.start()
	ioloop2 = server.ioloop
	print('Server started again')
	assert(ioloop == ioloop2)
	time.sleep(5)

	server.stop()
	print('Server stopped')

	server = Server()
	server.start()
	print('Third server start')
	ioloop.stop()
	print('Only ioloop stops but not the server')
	# time.sleep(5)

	print('To start only ioloop but not the server')
	ioloop.start()
	print('Only ioloop starts but not the server')
	time.sleep(5)

	server.stop()
	print('Third Server stopped')
