# TensorOnSpark
Running Tensorflow on Spark in the scalable, fast and compatible style

TensorOnSpark facilitates the researchers and programmer to easily write the regular Tensorflow programs and run Tensorflow on the Spark distributed computing paradigm. TensorOnSpark is innovated by the SparkSession, which  parallelizes the Tensorflow sessions in different executors of Spark. SparkSession maintains a reliable central parameter server, which synchronizes the machine learning model parameters periodically with the worker executors. The [TensorOnSpark Wiki page] (https://github.com/liangfengsid/tensoronspark/wiki/TensorOnSpark-for-Distributed-Deep-Learning) introduces the overview of the TensorOnSpark architecture.

##Programming example
TensorOnSpark program is very easy to write if one is already familiar with Tensorflow. An complete example of writing the MNIST program can be checked out in src/example/spark_mnist.py.
```
#initialize the learning model exactly as Tensorflow
import tensorflow as tf
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

#Extra information to notify the SparkSession about the input/output tensor and the variables.
feed_name_list = [x.name, y_.name]
param_list = [W, b]

#Initialize the SparkSession and run it with the Spark RDD data. 
spark_sess = sps.SparkSession(sc, sess, user='liangfengsid', name='spark_mnist', server_host='localhost', server_port=10080, sync_interval=100, batch_size=100)
spark_sess.run(train_step, feed_rdd=image_label_rdd, feed_name_list=feed_name_list, param_list=param_list, shuffle_within_partition=True)
```

##Brief Installation Instruction (Linux or Mac OS):

###Install Tensorflow in each computer
https://www.tensorflow.org/versions/r0.9/get_started/os_setup.html

### Setup the Hadoop and Spark cluster 
Hadoop: http://hadoop.apache.org

Spark: http://spark.apache.org
  
###Install TornadoWeb in each computer (Optional if the anaconda python is used). 
http://www.tornadoweb.org/en/stable/

###Install TensorOnSpark:
(1) Download the source from Github, compile and install it via:
```
$ python setup.py build
$ python setup.py install
```
(2) or install via the python repository, but this method may not install the latest version and may encounter unpredictable bugs. 
```
$ easy_install tensorspark
```
(3) For those that want to modify the code to adapt to its new project, they can just download the source code from Github without compiling and installing, and open the python shell under the directory "tensowflow/". 
  
###Configure the Spark cluster for TensorOnSpark

In the Spark configuration file, conf/spark-defaults.conf, add the following configuration information
```
#The directory in HDFS to store the SparkSession temporary files
spark.hdfs.dir 	/data
#The directory in the local computer to store the SparkSession temporary files
spark.tmp.dir 	/tmp
```

In the Hadoop configuration file under the Hadoop home directory, etc/hadoop/core-site, add the property for WebHDFS:
```
<!-- webhdfs setup -->
<property>
  <name>dfs.webhdfs.enabled</name>
  <value>true</value>
</property>
```

###Create the corresponding directory in HDFS configured in the previous step
```
bin/hadoop fs -mkdir /data
```

##Start with an example
###Prepare the MNIST example data and upload them to HDFS
Download the MNIST train data file in this Github under: src/MNIST_data/. 

Upload them to HDFS:
```
hadoop fs -put MNIST_data/* /data
```

###Run the MNIST example
Run Spark pyspark via the shell.
```
pyspark --deploy-mode=client
>>>import tensorspark.example.spark_mnist as mnist
>>># The 'user' parameter is the user name of HDFS
>>>mnist.train(sc=sc, user='liangfengsid', name='mnist_try', server_host='localhost', server_port=10080, sync_interval=100, batch_size=100, num_partition=1, num_epoch=2)
```



