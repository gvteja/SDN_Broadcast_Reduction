'''
/*******************************************************************************
 *   Copyright 2014 Karishma Sureka , Sai Gopal , Vijay Teja
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/
'''

import pox.openflow.libopenflow_01 as of
from pox.core import core
import pox
import time
log = core.getLogger("Global_Stats")

rx_packets = 0
tx_packets = 0
rx_dropped = 0
tx_dropped = 0
rx_errors = 0
tx_errors = 0
n = 0
cn = 0

#maybe reg and unreg listener in global stats fn so that we wont count the normal switch stats. better would be to have a bool map of all the switches that have recvd stats

def global_stats():
	global n
	global cn
	cn = 0
	n = core.openflow.connections.keys().__len__()
	log.info("sending port stats req to all {0} switches".format(n))
	for con in core.openflow.connections:
		con.send(of.ofp_stats_request(body=of.ofp_port_stats_request()))
	time.sleep(1)
	while(cn != n):
		time.sleep(1)
	log.info('global stats :\nrx_packets : {0} tx_packets : {1} rx_dropped : {2} tx_dropped : {3} rx_errors : {4} tx_errors : {5}'.format(rx_packets, tx_packets ,rx_dropped ,tx_dropped ,rx_errors ,tx_errors))

def stats_listener(event):
	#log.info(type(event))
	global cn
	global rx_packets
	global tx_packets
	global rx_dropped
	global tx_dropped
	global rx_errors
	global tx_errors
	cn = cn + 1
	log.info("switch " + str(event.connection.dpid) + " port statistics")
	for port_stats in event.stats:
		if(port_stats.port_no != of.OFPP_LOCAL):#no stats for openflow local port.
			rx_packets += port_stats.rx_packets
			tx_packets += port_stats.tx_packets
			rx_dropped += port_stats.rx_dropped
			tx_dropped += port_stats.tx_dropped
			rx_errors += port_stats.rx_errors
			tx_errors += port_stats.tx_errors

			log.info('port : {0} rx_packets : {1} tx_packets : {2} rx_dropped : {3} tx_dropped : {4} rx_errors : {5} tx_errors : {6}'.format(port_stats.port_no, port_stats.rx_packets, port_stats.tx_packets ,port_stats.rx_dropped ,port_stats.tx_dropped ,port_stats.rx_errors ,port_stats.tx_errors))
	


def launch():
	core.Interactive.variables['gstats'] = global_stats
	core.openflow.addListenerByName("PortStatsReceived", stats_listener)
