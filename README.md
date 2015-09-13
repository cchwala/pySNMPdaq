# Python SNMP data acquisition system for commercial microwave links

This is a package for real-time data acquisition of commercial microwave links via SNMP

# Installation

Install the dependencies. Clone this repository or download it as ZIP file

## Dependencies

 * numpy
 * pandas
 * netsnmp (Python bindings) 

## Installing SNMP

### on MacOS

* Install SNMP package via macports

* Compile the SNMP python bindings (**Important note**: This has to be redone when python environments are switched)
 
 - cd /opt/local/var/macports/distfiles/net-snmp/net-snmp-5.7.2/python/
 - python setup.py build
 - python setup.py test (requires a locally running agent w/ config provided)
 - python setup.py install# Python SNMP data acquisition system for commercial microwave links

### on Debian

* As root, install the relevant snmp packages

 - apt-get install snmp
 - apt-get install libsnmp-python
 
* if a non standard python path (e.g. because of a virtual environment) is used, copy over the files from SYSTEM-PYTHON-PATH/dist-packages
* to get the MIBS follow instructions from [https://wiki.debian.org/SNMP](here) 
