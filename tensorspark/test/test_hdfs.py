import tensorspark.core.hdfs_util as hdfs
import os
import time

def test_hdfs_get():
	host = "localhost"
	user = "liangfengsid"
	path = "/data/session_mnist_try_1475209287727.meta"
	local_dir = "/tmp"
	local_path = hdfs.get(host, user, path, local_dir)

	true_time = 0
	false_time = 0
	while True:
		if os.path.exists(local_path):
			true_time = true_time + 1
			break
		else:
			false_time = false_time + 1
			if false_time > 10:
				break
			time.sleep(0.1)

	print("True time = %d, False Time = %d" % (true_time, false_time))
	assert(false_time == 0)
