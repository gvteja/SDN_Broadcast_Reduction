'''
/*******************************************************************************
 *   Copyright 2014-2015 Karishma Sureka , Sai Gopal , Vijay Teja
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
from pox.lib.recoco import Timer

log = core.getLogger("Global_Stats")

rx_packets = 0
tx_packets = 0
rx_dropped = 0
tx_dropped = 0
rx_errors = 0
tx_errors = 0
num_sw = 0 #total num of switches
num_done = 0 #num of switches for whom stats have been collected
wait_timeout = 0
handler = {} #{dpid:portstats listner handler}



def _init():
    global num_sw
    global num_done
    global handler
    global rx_packets
    global tx_packets
    global rx_dropped
    global tx_dropped
    global rx_errors
    global tx_errors
    handler = {}
    num_done = 0
    num_sw = len(core.openflow.connections)
    rx_packets = 0
    tx_packets = 0
    rx_dropped = 0
    tx_dropped = 0
    rx_errors = 0
    tx_errors = 0


def global_stats():
    _init()
    log.info("Sending port stats req to all {0} switches".format(num_sw))
    for con in core.openflow.connections:
        handler[con.dpid] = con.addListenerByName("PortStatsReceived", _stats_listener)
        con.send(of.ofp_stats_request(body=of.ofp_port_stats_request()))
    Timer(wait_timeout, _check)

def _check():
    if num_done != num_sw: #means some stats not recvd for some switches. print anyway
        print_stats()

def _stats_listener(event):
    event.connection.removeListener(handler[event.dpid])
    global num_done
    global rx_packets
    global tx_packets
    global rx_dropped
    global tx_dropped
    global rx_errors
    global tx_errors
    num_done += 1
    log.debug('Received port statistics for switch {0}'.format(event.dpid))
    for port_stats in event.stats:
        if(port_stats.port_no != of.OFPP_LOCAL):#no stats for openflow local port.
            rx_packets += port_stats.rx_packets
            tx_packets += port_stats.tx_packets
            rx_dropped += port_stats.rx_dropped
            tx_dropped += port_stats.tx_dropped
            rx_errors += port_stats.rx_errors
            tx_errors += port_stats.tx_errors

        #log.debug('port : {0} rx_packets : {1} tx_packets : {2} rx_dropped : {3} tx_dropped : {4} rx_errors : {5} tx_errors : {6}'.format(port_stats.port_no, port_stats.rx_packets, port_stats.tx_packets ,port_stats.rx_dropped ,port_stats.tx_dropped ,port_stats.rx_errors ,port_stats.tx_errors))
    if num_done == num_sw: #check if all switches are done
        print_stats()

def print_stats():
    log.info('Aggregated switch stats :\nrx_packets : {0} tx_packets : {1} rx_dropped : {2} tx_dropped : {3} rx_errors : {4} tx_errors : {5}'.format(rx_packets, tx_packets ,rx_dropped ,tx_dropped ,rx_errors ,tx_errors))


def launch(wait_time = 30):
    global wait_timeout
    wait_timeout = int(wait_time)
    core.Interactive.variables['gstats'] = global_stats
