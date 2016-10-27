#!/usr/bin/env python

import os
try:
	from setuptools import setup
except ImportError:
	from distutils.core import setup

setup(
	name = "tensorspark",
	version = "1.0.6",
	author = "Liang Feng",
	author_email = "loengf@connect.hku.hk",
	description = ("Tensorflow on Spark, a scalable system for high-performance machine learning"),
	license = "Apache",
	keywords = "",
	url = "https://github.com/liangfengsid/tensoronspark/",
	packages=['tensorspark', 'tensorspark.core', 'tensorspark.example', 'tensorspark.test'],
	package_data={'tensorspark':['MNIST_data/*-ubyte']}, 
	classifiers=[
		'Development Status :: 4 - Beta',
		'Topic :: System :: Distributed Computing',
		'Topic :: Scientific/Engineering :: Artificial Intelligence',
		'License :: OSI Approved :: Apache Software License',
		'Programming Language :: Python :: 2.7',
		'Operating System :: POSIX :: Linux', 
		'Operating System :: Unix',
		'Environment :: Console',
	],
)