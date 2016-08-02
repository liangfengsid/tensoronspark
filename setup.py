#!/usr/bin/env python

import os
try:
	from setuptools import setup
except ImportError:
	from distutils.core import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
	return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
	name = "tensorspark",
	version = "1.0.1",
	author = "Liang Feng",
	author_email = "loengf@connect.hku.hk",
	description = ("Tensorflow on Spark, a scalable system for high-performance machine learning"),
	license = "Apache",
	keywords = "",
	url = "https://github.com/liangfengsid/tensorspark/",
	package_dir={'tensorspark':'src'},
	packages=['tensorspark', 'tensorspark.core', 'tensorspark.example'],
	package_data={'tensorspark':['MNIST_data/*-ubyte']}, 
	long_description=read('README.md'),
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