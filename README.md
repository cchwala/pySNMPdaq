# Python SNMP data acquisition system for commercial microwave links

This is a package to acquire microwave link data from Ericsson Traffic Node (TN) hardware via SNMP

# Installation

Clone this repository and install the dependencies

## Dependencies

 * numpy
 * pandas
 * netsnmp (Python bindings) 

## Installing SNMP on MacOS (at least on my machine)

Install SNMP package via macports

 - blablabla
 
Compile the python bindings (this has to be redone when python environments are swithched)
 
 - cd /opt/local/var/macports/distfiles/net-snmp/net-snmp-5.7.2/python/
 - python setup.py build
 - python setup.py test (requires a locally running agent w/ config provided)
 - python setup.py install# Python SNMP data acquisition system for commercial microwave links

## Installing SNMP on Debian

As root, install the relevant snmp packages

 - apt-get install snmp
 - apt-get install libsnmp-python
 
 - if a non standard python path (e.g. because of a virtual environment) is used, copy over the files from SYSTEM-PYTHON-PATH/dist-packages
 - to get the MIBS follow instructions from [https://wiki.debian.org/SNMP](here) 
