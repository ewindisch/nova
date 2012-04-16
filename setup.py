# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright 2011 Cloudscaling Group, Inc
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you
#    may
#    not use this file except in compliance with the License. You may
#    obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the
#    License for the specific language governing permissions and
#    limitations
#    under the License.

import os
import sys
from setuptools import setup, find_packages

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "cloudscaling.nova.rpc.impl_zmq",
    version = "1.0",
    author = "Eric Windisch",
    author_email = "eric@cloudscaling.com",
    description = ("The OpenStack Nova RPC ZeroMQ driver"),
    license = "Apache 2.0",
    keywords = "OpenStack Nova RPC ZeroMQ driver",
    url = "TODO",
    packages = find_packages(exclude=['bin']),
    zip_safe=False,
    install_requires=['nova'],
    long_description=read('README.md'),
    classifiers=[
        "Environment :: Plugins",
        "License :: OSI Approved :: Apache Software License",
        ],
    scripts=['bin/nova-rpc-zmq',
             'bin/nova-rpc-zmq-receiver']
)
