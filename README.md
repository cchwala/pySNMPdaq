# `pySNMPdaq`

A flexible Python SNMP data acquisition system designed for hydrometeorological applications of commercial microwave links.

**Reference:**

Chwala, C., Keis, F., and Kunstmann, H.: Real-time data acquisition of commercial microwave link networks for hydrometeorological applications, Atmos. Meas. Tech., 9, 991-999, https://doi.org/10.5194/amt-9-991-2016, 2016. 

# Test it using a VirtualMachine (VM) image

**Note:** The version of `pySNMPdaq` on the VM is not up to date.  It does not support staggerd requests in defined batches. Hence, it should only be used for a small number of requests.

Install [VirtualBox](https://www.virtualbox.org/), dowload the [VM image](https://drive.google.com/open?id=0B-spzN5NShBEbGZVai1ZUEpaVEU) and start the VM via VirtualBox

The VM runs a minimal Debian installation with the following login:

 * username: daq-user
 * password: daq-user

`pySNMPdaq` is setup up with a test configuration. This test configuration only polls SNMP data localy (here, as example the, uptime) so that no remote access to a commecrial microwave link is needed. 

You will have to change the OID listing file to poll data from other IPs and change the respective OID entries to poll the data you want. An example OID listing file for polling TX and RX data from two Ericsson MINI-LINK Traffic Nodes (here, with random IP addresses) is available in the `pySNMPdaq` directory on the VM. 

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
* From the `pySNMPdaq` directory start the pySNMPdaq daemon with
 
 ```
 ./pySNMPdaqd.py start
 ```
* You can stop the daemon using the command
 
 ```
 ./pySNMPdaqd.py stop
 ```
 
The notebook [`Example for building the OID dict list for Ericsson MINI LINK TN systems`](https://github.com/cchwala/pySNMPdaq/blob/master/notebooks/Example%20for%20building%20the%20OID%20dict%20list%20for%20Ericsson%20MINI%20LINK%20TN%20systems.ipynb) shows how to generate the necessary OID dict list and provides functions to ease this task for Ericsson MINI LINK TN systems.
