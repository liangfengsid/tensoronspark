import numpy as np
import struct
import tensorflow as tf
import tensorflow.python.framework.dtypes as dtypes
import pyspark
import random

import tensorspark.core.spark_session as sps
import tensorspark.core.partitioner as par
import tensorspark.core.weight_combiner as comb

def extract_images(sc, filepath, dtype=dtypes.float32, reshape=True):
	"""Extract the images into a 4D uint8 numpy array [index, y, x, depth]."""

	def _extract_single_image_file(iterator):
		file_image_map = []
		while True: 
			try:
				pathData = iterator.next()
				filename = pathData[0]
				fileindex = filename.split('-')[-2]
				data = pathData[1]

				image_meta = struct.unpack('>iiii', buffer(data, 0, 16))
				magic = image_meta[0]
				if magic !=2051:
					raise ValueError('Invalid magic number %d in MNIST image file: %s' % (magic, filename))
				num_images = image_meta[1]
				rows = image_meta[2]
				cols = image_meta[3]
				buf = buffer(data, 16)
				images = np.frombuffer(buf, dtype=np.uint8)
				images = images.reshape(num_images, rows, cols, 1)

				# dt = np.dtype(np.uint32).newbyteorder('>')
				# magic = np.fromstring(data[0:4], dt)[0]
				# if magic !=2051:
				# 	raise ValueError('Invalid magic number %d in MNIST image file: %s' % (magic, filename))
				# numImages = np.fromstring(data[4:8], dtype=dt)[0]
				# rows = np.fromstring(data[8:12], dtype=dt)[0]
				# cols = np.fromstring(data[12:16], dtype=dt)[0]
				# images = np.fromstring(data[16:], dtype=np.uint8)
				# images = images.reshape(numImages, rows, cols, 1)

				if reshape == True:
					images = images.reshape(num_images, rows * cols)

				if dtype == dtypes.float32:
					images = np.multiply(images, 1.0 / 255.0)
				file_image_map.append((fileindex, images))
			except StopIteration:
				break
		return file_image_map
		#mnist_train(images)
		# return [images]

	# import struct
	data_rdd = sc.binaryFiles(filepath)
	image_rdd = data_rdd.mapPartitions(_extract_single_image_file)
	return image_rdd



def extract_labels(sc, filepath, num_class, one_hot=False):

	def _extract_single_label_file(iterator):
		file_label_map = []
		while True:
			try:
				pathData = iterator.next()
				filename = pathData[0]
				fileindex = filename.split('-')[-2]
				data = pathData[1]

				label_meta = struct.unpack('>ii', buffer(data, 0, 8))
				magic = label_meta[0]
				if magic !=2049:
					raise ValueError('Invalid magic number %d in MNIST image file: %s' % (magic, filename))
				num_labels = label_meta[1]
				buf = buffer(data, 8)
				labels = np.frombuffer(buf, dtype=np.uint8)
				if one_hot:
					labels = dense_to_one_hot(labels, num_class)

				# dt = np.dtype(np.uint32).newbyteorder('>')
				# magic = np.fromstring(data[0:4], dt)[0]
				# if magic != 2049:
				# 	raise ValueError('Invalid magic number %d in MNIST image file: %s' % (magic, filename))
				# num_items = mp.fromstring(data[4:8], dt)[0]
				# labels = np.fromstring(data[8:], dtype=np.uint8)
				file_label_map.append((fileindex,labels))
			except StopIteration:
				break
		return file_label_map

	data_rdd = sc.binaryFiles(filepath)
	label_rdd = data_rdd.mapPartitions(_extract_single_label_file)
	return label_rdd


def dense_to_one_hot(labels, num_class):
	"""Convert class labels from scalars to one-hot vectors."""
	num_labels = labels.shape[0]
	index_offset = np.arange(num_labels) * num_class
	labels_one_hot = np.zeros((num_labels, num_class))
	labels_one_hot.flat[index_offset + labels.ravel()] = 1
	return labels_one_hot


def flatten_image_label(iteration):
	image_label_list = []
	while True:
		try:
			file_image_label = iteration.next()
			images = file_image_label[1][0]
			labels = file_image_label[1][1]
			assert (images.shape[0] == labels.shape[0])
			for i in xrange(images.shape[0]):
				image_label_list.append((images[i], labels[i]))
		except StopIteration:
			break
	return image_label_list


data_dir = '/data/mnist/'  # Should be some file on hdfs
train_image_path=data_dir+'train-images-idx1-ubyte'
train_label_path=data_dir+'train-labels-idx1-ubyte'
test_image_path=data_dir+'t10k-images-idx1-ubyte'
test_label_path=data_dir+'t10k-labels-idx1-ubyte'


class RandomPartitioner(object):
	def __init__(self, num_partition):
		self.num_partition = num_partition


	def __eq__(self, other):
		return (isinstance(other, RandomPartitioner) and self.num_partition == other.num_partition)


	def __call__(self, k):
		return random.randint(0, num_partition - 1)


def train(sc=None, user=None, name='spark_mnist', server_host='localhost', server_port=10080, sync_interval=100, batch_size=100, num_partition=1, num_epoch=1, server_reusable=True):
	is_new_sc = False
	if sc is None:
		sc = pyspark.SparkContext(conf=pyspark.SparkConf())
		is_new_sc = True

	image_rdd = extract_images(sc, train_image_path)
	label_rdd = extract_labels(sc, train_label_path, num_class=10, one_hot=True)
	image_label_rdd = image_rdd.join(label_rdd, numPartitions=num_partition).mapPartitions(flatten_image_label).cache()

	x = tf.placeholder(tf.float32, [None, 784])
	W = tf.Variable(tf.zeros([784, 10]))
	b = tf.Variable(tf.zeros([10]))
	y = tf.nn.softmax(tf.matmul(x, W) + b)
	y_ = tf.placeholder(tf.float32, [None, 10])
	cross_entropy = tf.reduce_mean(-tf.reduce_sum(y_ * tf.log(y), reduction_indices=[1]))
	train_step = tf.train.GradientDescentOptimizer(0.5).minimize(cross_entropy)
	init = tf.initialize_all_variables()
	sess = tf.Session()
	sess.run(init)

	feed_name_list = [x.name, y_.name]
	param_list = [W, b]

	spark_sess = sps.SparkSession(sc, sess, user=user, name=name, server_host=server_host, server_port=server_port, sync_interval=sync_interval, batch_size=batch_size)
	partitioner = par.RandomPartitioner(num_partition)
	combiner = comb.DeltaWeightCombiner()
	for i in range(num_epoch):
		spark_sess.run(train_step, feed_rdd=image_label_rdd, feed_name_list=feed_name_list, param_list=param_list, weight_combiner=combiner, shuffle_within_partition=True, server_reusable=server_reusable)
		if i != num_epoch-1:
			temp_image_label_rdd = image_label_rdd.partitionBy(num_partition, partitioner).cache()
			image_label_rdd.unpersist()
			image_label_rdd = temp_image_label_rdd

	# Since the parameter server is reusable in this spark_sess.run() example, one should stop the parameter server manually when it is no long used. 
	spark_sess.stop_param_server()

	if is_new_sc:
		sc.close()

	from tensorflow.examples.tutorials.mnist import input_data
	mnist_data = input_data.read_data_sets("MNIST_data/", one_hot=True)
	correct_prediction = tf.equal(tf.argmax(y,1), tf.argmax(y_,1))
	accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))
	print(sess.run(accuracy, feed_dict={x: mnist_data.test.images, y_: mnist_data.test.labels}))

	return sess


