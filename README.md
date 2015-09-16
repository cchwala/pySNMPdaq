#`pySNMPdaq`

A flexible Python SNMP data acquisition system designed for hydrometeorological applications of commercial microwave links.

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

# Usage

* Adjust the configuration for logging, file names, directories, file transfer, etc. in `config.py`.
* Put the list of IP addresse and OIDs in the file `mw_link_OID_listing.py`, which currently is configured to only acquire the uptime of `localhost` as an example that can be run on your local machine without access to a remote machine. An example of a listing for two microwave links is given in the file `mw_link_OID_listing_example.py`.
* Start the pySNMPdaq daemon with
 
 ```
 pySNMPdaqd start
 ```
* You can stop the daemon using hte command
 
 ```
 pySNMPdaqd stop
 ```
