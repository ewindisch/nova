import os
import sys
from setuptools import setup, find_packages

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "nova.rpc.impl_zmq",
    version = "1.0",
    author = "Eric Windisch",
    author_email = "eric@cloudscaling.com",
    description = ("The OpenStack Nova RPC ZeroMQ driver"),
    license = "Apache 2.0",
    keywords = "OpenStack Nova RPC ZeroMQ driver",
    url = "TODO",
    packages = find_packages(),
    zip_safe=False,
    install_requires=['nova'],
    long_description=read('README.md'),
    classifiers=[
        "Environment :: Plugins",
        "License :: OSI Approved :: Apache Software License",
        ],
)
