#!/usr/bin/env python

import pprint

timers = [1, 2]
hosts = ['localhost', ]
OID_dict = {'uptime_1': '.1.3.6.1.2.1.1.3.0',
            'uptime_2': '.1.3.6.1.2.1.1.3.0'}
ID_prefix = 'test'

N_per_timer_and_host = 500

mw_link_list = []

for timer in timers:
    for i in range(N_per_timer_and_host):
        for host in hosts:
            mw_link_list.append(
                {'ID': ID_prefix + '_' + str(timer) + '_' + str(i),
                 'IP': host,
                 'OID_dict': OID_dict,
                 'timer': timer
                 }
            )

with open('mw_link_OID_listing.py', 'w') as fout:
    fout.write('# Example file for testing generated '
               'by "generate_test_OID_listing.py"\n\n')
    fout.write('mw_link_list = ')
    fout.write(pprint.pformat(mw_link_list))
