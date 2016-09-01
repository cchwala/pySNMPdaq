#################################################
# Example config to test pySNMPdaq on localhost #
#################################################
#
# 'ID', 'IP',  and 'OID_dict' are mandatory. Additional keys may
# be added if needed.
#

mw_link_list = \
[{'ID': u'test_1_1',
  'IP': u'localhost',
  'OID_dict': {'uptime_1': '.1.3.6.1.2.1.1.3.0',
               'uptime_2': '.1.3.6.1.2.1.1.3.0'},
  'protection': False,
  'slot': '2',
  'timer': 1},
 {'ID': u'test_1_2',
  'IP': u'localhost',
  'OID_dict': {'uptime_1': '.1.3.6.1.2.1.1.3.0',
               'uptime_2': '.1.3.6.1.2.1.1.3.0'},
  'protection': False,
  'slot': '2',
  'timer': 1},
 {'ID': u'test_2_1',
  'IP': u'localhost',
  'OID_dict': {'uptime_1': '.1.3.6.1.2.1.1.3.0',
               'uptime_2': '.1.3.6.1.2.1.1.3.0'},
  'protection': False,
  'slot': '2',
  'timer': 2},
 {'ID': u'test_2_2',
  'IP': u'localhost',
  'OID_dict': {'uptime_1': '.1.3.6.1.2.1.1.3.0',
               'uptime_2': '.1.3.6.1.2.1.1.3.0'},
  'protection': False,
  'slot': '2',
  'timer': 2}
 ]
