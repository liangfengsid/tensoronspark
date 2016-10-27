import tensorflow as tf

def test_save_restore():
	tf.reset_default_graph()
	sess = tf.Session()
	path = '/tmp/tensor_saved_test2'
	meta_path = path + '.meta'

	r = tf.train.import_meta_graph(meta_path)
	r.restore(sess, path)

	s = _get_saver(100)
	s.save(sess, path)


	tf.reset_default_graph()
	sess2 = tf.Session()

	r2 = tf.train.import_meta_graph(meta_path)
	r2.restore(sess2, path)
	s2 = _get_saver(100)
	s2.save(sess2, path)


def _get_saver(timeout):
	retry = timeout
	while retry > 0:
		try:
			saver = tf.train.Saver()
			return saver
		except AssertionError as e:
			print('Retry creating Saver() due to the unnecessary assertion in Tensorflow r10 and earlier.')
			retry =  retry - 1
			if retry == 0:
				raise TypeError('Saver cannnot be created due to the unnecessary assertion in Tensorflow r10 and earlier. Retry or update Tensorflow to the latest to fix.')


def test_saver():
	from tensorflow.examples.tutorials.mnist import input_data
	mnist_data = input_data.read_data_sets("MNIST_data/", one_hot=True)

	x = tf.placeholder(tf.float32, [None, 784])
	W = tf.Variable(tf.zeros([784, 10]))
	# cW = tf.Variable(tf.zeros([784, 10]))
	b = tf.Variable(tf.zeros([10]))

	y = tf.nn.softmax(tf.matmul(x, W) + b)

	y_ = tf.placeholder(tf.float32, [None, 10])
	cross_entropy = tf.reduce_mean(-tf.reduce_sum(y_ * tf.log(y), reduction_indices=[1]))

	train_step = tf.train.GradientDescentOptimizer(0.5).minimize(cross_entropy)
	init = tf.initialize_all_variables()
	sess = tf.Session()
	sess.run(init)

	for i in range(1000):
		batch_xs, batch_ys = mnist_data.train.next_batch(100)
		sess.run(train_step, feed_dict={x: batch_xs, y_: batch_ys})

	correct_prediction = tf.equal(tf.argmax(y,1), tf.argmax(y_,1))
	accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

	saved_path = '/tmp/tensor_saved_test'
	with sess.as_default():
		saver = tf.train.Saver()
		saver.save(sess, saved_path)

	saved_meta_path = saved_path + '.meta'

	sess2 = tf.Session()
	sess2.as_default()
	restorer = tf.train.import_meta_graph(saved_meta_path)
	restorer.restore(sess2, saved_path)
	print(sess2.run(accuracy, feed_dict={x: mnist_data.test.images, y_: mnist_data.test.labels}))
	saver = tf.train.Saver()
	# saver.save(sess2, saved_path)




