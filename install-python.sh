#!/bin/bash
#---------------------------------------------------------------#
yum install -y xz-devel
yum install libffi-devel

#---------------------------------------------------------------#
cd /usr/src
wget https://www.python.org/ftp/python/3.7.4/Python-3.7.4.tgz
tar xzf Python-3.7.4.tgz
cd Python-3.7.4
./configure --enable-optimizations
make altinstall

#---------------------------------------------------------------#
pip3.7 install pylzma


