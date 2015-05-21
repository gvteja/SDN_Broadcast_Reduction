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

from pox.core import core
import pox.openflow.libopenflow_01 as of
import pox.lib.packet as pkt
import pox.lib.addresses as adrs
import pox.openflow.nicira as nx
from pox.lib.revent import *
from pox.openflow.discovery import Discovery
from pox.lib.util import dpidToStr
from pox.lib.recoco import Timer
from time import time
import math
import random

log = core.getLogger("****SDN APP****")

num_pods = -1
num_switches = 0
total_switches = 0
cur_num_links = 0

total_num_links = 0
max_connected_perc = 50
setup_time = 0
link_timeout = 0
arp_cache_timeout = 2


started = False
last_link_event = 0

#store dpids
all_switches = set()
edge_switches = set()
agg_switches = set()
core_switches = set()

total_ports = {}

#pod:[{pos:dpid}]
cs = []
ags = []
egs = []

switch_pos = {} #{dpid:[pod,pos]}
assigned_pmac = {}  #{dpid:[]}
ports = {}

g = {}  #{dpid:{src_port:[dst_dpid, dst_port]}}

agg_up_ports = {}
agg_down_ports = {}
host_ports = {}

pod_port = {} #{ core_switch_dpid : list(index is pod_num) of ports on core switch conected to that pod }
port_pod = {} #{ core_switch_dpid : {src port : pod that port is connected to} }
mask_ports = {} #{ core_switch_dpid : { mask of port num : [ ports to each pod for that mask after hashing ] } }

latent_host_pos = {} #{ actual mac : (edge dpid, port) }

events = {}

arp_table = {} #ip->pmac
pmac_actual = {}
actual_pmac = {}

init_asgn_str = '1' + ('0' * 254) + '1'
zero_ip = pkt.IPAddr('0.0.0.0')

count = 1


def _handle_ConnectionUp (event):
    #When a switch connects, create a new node in graph
    global num_switches
    num_switches += 1
    #g[event.dpid] = [ [-1,-1] for i in range(num_pods)]
    g[event.dpid] = {}
    switch_pos[event.dpid] = [-1, -1]
    l = [port.port_no for port in event.ofp.ports]
    if of.OFPP_LOCAL in l:  #removing the openflow local port
        l.remove(of.OFPP_LOCAL)
    total_ports[event.dpid] = len(l)
    ports[event.dpid] = l 
    all_switches.add(event.dpid)
    # Turn on ability to specify table in flow_mods
    msg = nx.nx_flow_mod_table_id()
    event.connection.send(msg)

def _handle_LinkEvent (event):
    # When links change, update graph
    global cur_num_links
    link = event.link
    try:
        if(event.added):    #new link
            g[link.dpid1][link.port1] = [link.dpid2, link.port2]
            #g[link.dpid2][link.port2] = [link.dpid1, link.port1]
            cur_num_links += 1
            if link_timeout:
                global last_link_event
                last_link_event = int(time())
                Timer(link_timeout, _check)#read recoco implem and see if you can cancel this timer. this will unncessarily call _check many times
    except Exception as e:        
        log.fatal('link update failed for link:{0} type:{1} message:{2} args:{3} dpid1:{4} port1:{5} dpid2:{6} port2:{7}'.format(str(link), type(e), e.message, e.args, link.dpid1, link.port1, link.dpid2, link.port2) )
    if event.added and total_num_links and (cur_num_links == total_num_links):
        #print '###############started coz of num of links'
        _start()

def _ready():
    if setup_time:
        #print '###############started coz of setup time'
        Timer(setup_time, _start) #time before the topology discovery starts

def _check():
    if ( not started and ( int(time()) - last_link_event ) >= link_timeout ):
        #print '###############started coz of link timeout'
        _start()

def _start():
    global started
    if started:
        return
    started = True
    print 'App initialization started'
    log.info( 'Starting App initialization : num_switches:{0}, num_links:{1}'.format(num_switches, cur_num_links) )
    global core_switches
    log.info( 'Graph : {0}'.format(g))
    for dpid,links in g.iteritems():
        if ( len(links) / float(total_ports[dpid]) ) <= max_connected_perc:  
            edge_switches.add(dpid)
            assigned_pmac[dpid] = {}
            host_ports[dpid] = list( set(ports[dpid]) - set(g[dpid].keys()) )
            for port in host_ports[dpid]:#for all the edge host ports, init the assigned pmac string
                    assigned_pmac[dpid][port] = init_asgn_str
    for dpid in edge_switches:  #traverse through all links of the edge switch
        links = g[dpid]
        for src_port,link in links.iteritems():
            if link[0] not in agg_switches: #1st time we see this agg, add it and init the down ports list
                agg_switches.add(link[0])
                agg_down_ports[link[0]] = []
            agg_down_ports[link[0]].append(link[1])
    core_switches = all_switches.difference(agg_switches.union(edge_switches))
    #global cs
    #cs = list(core_switches)#not really req. in arp reply, we r sending to one core switch. maybe that itself not req. send to nearest edge switch? use pmac fn

    for dpid in agg_switches:
        agg_up_ports[dpid] = list( set(g[dpid].keys()) - set(agg_down_ports[dpid]) )

    log.info( 'Edge switches : {0}'.format(edge_switches) )
    log.info( 'Aggregate switches : {0}'.format(agg_switches) )
    log.info( 'Core switches : {0}'.format(core_switches) )

    '''
    bfs to get the pods.
    make all the dpids of core switch links in agg switch -ve. and do bfs on all agg switches
    the switch pos can used as visited or not. we can have global switch nos or sep switch nos for agg and edge. right now leaving it global. can change
    '''

    toggle_agg_core_links() #logically remove agg to core links

    pod = 0 #make it one here. and some changes later. like egs and ags are zero index pod num. maybe in bcast semanitcs also somewhere
    for agg in agg_switches:
        if switch_pos[agg][0] == -1 :   #new switch
            bfs(agg, pod)
            pod += 1
    global num_pods
    num_pods = pod

    toggle_agg_core_links() #renable links after discovering pods using bfs

    log.info( 'Nume of pods : {0} \nSwitch positions : {1}'.format( num_pods, switch_pos ) )

    insert_routes()
    for es in edge_switches:#for arp entries and wildcard resubmissions
        msg = nx.nx_flow_mod()
        msg.priority = 9000
        msg.match.eth_type = pkt.ethernet.ARP_TYPE
        msg.actions.append(of.ofp_action_output(port = of.OFPP_CONTROLLER))
        for port in host_ports[es]:
            msg.match.NXM_OF_IN_PORT = port
            core.openflow.connections[es].send(msg)
        msg = nx.nx_flow_mod(table_id = 0)
        msg.priority = 10
        msg.actions.append(nx.nx_action_resubmit.resubmit_table(table=1))
        core.openflow.connections[es].send(msg)
        msg = nx.nx_flow_mod(table_id = 1)
        msg.priority = 10
        msg.actions.append(nx.nx_action_resubmit.resubmit_table(table=2))
        core.openflow.connections[es].send(msg)

    #bcast semantics
    msg = nx.nx_flow_mod(table_id = 0)
    msg.priority = 8500
    msg.match.eth_dst = 'ff:ff:ff:ff:ff:ff'
    #agg switch
    for agg in agg_switches:
        con = core.openflow.connections[agg]
        num_down_links = len(agg_down_ports[agg])
        down_actions = [ of.ofp_action_output(port = dp) for dp in agg_down_ports[agg] ]
        #pkt downwards
        msg.actions = down_actions
        for in_port in agg_up_ports[agg]:
            msg.match.NXM_OF_IN_PORT = in_port
            con.send(msg)
        
        #for pkt upwrds
        num_up_links = len(agg_up_ports[agg])
        for i in range(num_down_links):
            msg.match.NXM_OF_IN_PORT = agg_down_ports[agg][i]
            msg.actions = list(down_actions)
            msg.actions.pop(i)
            #and add one up link here. assuming num of down links is more than or equal to num up links
            msg.actions.append(of.ofp_action_output(port = agg_up_ports[agg][ i % num_up_links ]))
            con.send(msg)
    
    #edge switch
    msg = nx.nx_flow_mod(table_id = 2)
    msg.priority = 8500
    msg.match.eth_dst = 'ff:ff:ff:ff:ff:ff'
    for es in edge_switches:
        con = core.openflow.connections[es]
        num_down_links = len(host_ports[es])
        down_actions = [ of.ofp_action_output(port = dp) for dp in host_ports[es] ]
        #pkt downwards
        msg.actions = down_actions
        for in_port in g[es]:
            msg.match.NXM_OF_IN_PORT = in_port
            con.send(msg)
        
        #for pkt upwrds
        num_up_links = len(g[es])
        _up_ports = g[es].keys()
        for i in range(num_down_links):
            msg.match.NXM_OF_IN_PORT = host_ports[es][i]
            msg.actions = list(down_actions)
            msg.actions.pop(i)
            #and add one up link here. assuming num of down links is more than or equal to num up links. ie the perc >= 50
            msg.actions.append(of.ofp_action_output(port = _up_ports[ i % num_up_links ]))
            con.send(msg)
    
    #core switch
    #flood in case of portland will work. in other cases, choose one of the op link to a pod.
    #have to take care of ctlr initiated bcast too, but we are not using it. so chucking it for now
    msg_cs = nx.nx_flow_mod(table_id = 0)
    msg_cs.priority = 5000 #higher priority than normal routing
    msg_cs.match.eth_dst = 'ff:ff:ff:ff:ff:ff'
    for core_switch in cs:
        con = core.openflow.connections[core_switch]
        links = g[core_switch]
        _pod_port = pod_port[core_switch]
        _mask_ports = mask_ports[core_switch]
        if not _mask_ports: #means this core switch has no redundant links at allm then flood. we assume that each core is connct to all pods.
            msg_cs.actions = [of.ofp_action_output(port = of.OFPP_FLOOD)]
            con.send(msg_cs)
            continue
        for src_port, link in links.iteritems():
            msg_cs.match.NXM_OF_IN_PORT = src_port
            msg_cs.actions = []
            src_pod = port_pod[core_switch][src_port]
#take care of pod with only one link. those wont have masks. so add them directly
            '''
            for pod_num in range(num_pods):#sent to all other pod except src pod
                if pod_num == src_pod:
                    continue
            '''
            for key,lports in _mask_ports.iteritems():
                msg_cs.actions = []
                dst_ports = list(lports)
                for port in _pod_port[src_pod]:
                    if port in dst_ports:
                        dst_ports.remove(port)#remove the ports leading back to src pod
                for l in _pod_port:
                    if len(l) == 1:
                        dst_ports.append(l[0])#add pods with only one link to this core switch. they wouldnt have been considered fr hashing prev
                msg_cs.match.eth_src_with_mask = key
                for _port in dst_ports:
                    msg_cs.actions.append(of.ofp_action_output(port = _port))
                con.send(msg_cs)

    #handle packet in after flow tables are installed and everything is set up
    core.openflow.addListenerByName("PacketIn", _handle_PacketIn)
    print 'App initialization done'

    
def insert_routes():
    #inserting frowarding tables into all the switches
    #maybe send barrier to all switches in the end
    for core_switch in core_switches:
        #each core switch coudl be con to more than 1 ags of a pod. so hash. ASSUMEPTION : a core switch is con to all pods
        switches = [[] for i in range(num_pods)]
        links = g[core_switch]
        _port_pod = {}
        _mask_ports = {}
        con = core.openflow.connections[core_switch]
        for src_port,link in links.iteritems():
            pod = switch_pos[link[0]][0]
            switches[pod].append(src_port)#appending src_port to pod no
            _port_pod[src_port] = pod
        pod_port[core_switch] = switches
        port_pod[core_switch] = _port_pod
        for pod_num in range(num_pods):                
            num_routes = len(switches[pod_num])
            if num_routes > 1:  #need hashing. hashing is based on port num
                msg_cs = nx.nx_flow_mod(table_id = 0)
                msg_cs.priority = 2000
                num_bits  = int(math.floor(math.log(num_routes, 2)))
                x = 2 ** num_bits   #Assumption:one core switch doesn't have more than 255 direct connections to a pod
                mask = 'ff:ff:00:' + eth_addr_format(x - 1, 2) + ':00:00'
                mask_for_bcast = '00:00:00:' + eth_addr_format(x - 1, 2) + ':00:00'
                prefix = eth_addr_format(pod_num, 4) + ':00:'
                suffix = ':00:00'
                for i in range(num_routes):
                    port = i % x
                    port_mask = prefix + eth_addr_format(port, 2) + suffix
                    msg_cs.match.eth_dst_with_mask = ( port_mask, mask)
                    dst_port = switches[pod_num][i]
                    msg_cs.actions = [of.ofp_action_output(port = dst_port)]
                    key = ('00:00:00:' + eth_addr_format(port, 2) + ':00:00', mask_for_bcast)
                    if key not in _mask_ports:
                        _mask_ports[key] = []
                    _mask_ports[key].append(dst_port)
                    con.send(msg_cs)
            else:
                msg_cs = nx.nx_flow_mod(table_id = 0)
                msg_cs.priority = 2000
                msg_cs.match.eth_dst_with_mask = ( eth_addr_format(pod_num, 4) + ":00:00:00:00", "ff:ff:00:00:00:00")
                msg_cs.actions.append(of.ofp_action_output(port = switches[pod_num][0]))
                con.send(msg_cs)
        mask_ports[core_switch] = _mask_ports

    for agg_switch in agg_switches:
        con = core.openflow.connections[agg_switch]
        up_ports = agg_up_ports[agg_switch]
        num_routes = len(up_ports)
        if num_routes > 1:  #need hashing. hashing is based on port num
            msg_as = nx.nx_flow_mod(table_id = 0)
            msg_as.priority = 2000
            num_bits  = int(math.floor(math.log(num_routes, 2)))
            x = 2 ** num_bits   #Assumption:one agg switch doesn't have more than 255 direct connections to core switches
            mask = '00:00:00:' + eth_addr_format(x - 1, 2) + ':00:00'
            prefix = '00:00:00:'
            suffix = ':00:00'
            for i in range(num_routes):
                port = i % x
                msg_as.match.eth_dst_with_mask = ( prefix + eth_addr_format(port, 2) + suffix, mask)
                msg_as.actions = [ of.ofp_action_output(port = up_ports[i]) ]
                con.send(msg_as)
        else:
            msg_as = nx.nx_flow_mod(table_id = 0)
            msg_as.priority = 2000
            msg_as.actions.append(of.ofp_action_output(port = up_ports[0]))
            con.send(msg_as)
      
        down_ports = agg_down_ports[agg_switch]
        pod_num = switch_pos[agg_switch][0]
        prefix = eth_addr_format(pod_num , 4) + ':'
        suffix = ':00:00:00'
        mask = 'ff:ff:ff:00:00:00'
        msg_as = nx.nx_flow_mod(table_id = 0)
        msg_as.priority = 3000

        for src_port in down_ports:
            pos = switch_pos[g[agg_switch][src_port][0]][1]
            msg_as.match.eth_dst_with_mask = ( prefix + eth_addr_format(pos, 2) + suffix, mask)
            msg_as.actions = [ of.ofp_action_output(port = src_port) ]
            con.send(msg_as)
    
    for edge_switch in edge_switches:
        con = core.openflow.connections[edge_switch]
        up_ports = g[edge_switch].keys()
        num_routes = len(up_ports)
        if num_routes > 1:  #need hashing. hashing is based on port num
            msg_es = nx.nx_flow_mod(table_id = 2)
            msg_es.priority = 2000
            num_bits  = int(math.floor(math.log(num_routes, 2)))
            x = 2 ** num_bits   #Assumption:one edge switch doesn't have more than 255 direct connections to agg switches
            mask = '00:00:00:' + eth_addr_format(x - 1, 2) + ':00:00'
            prefix = '00:00:00:'
            suffix = ':00:00'
            for i in range(num_routes):
                port = i % x
                msg_es.match.eth_dst_with_mask = ( prefix + eth_addr_format(port, 2) + suffix, mask)
                msg_es.actions = [ of.ofp_action_output(port = up_ports[i]) ]
                con.send(msg_es)
        else:
            msg_es = nx.nx_flow_mod(table_id = 2)
            msg_es.priority = 2000
            msg_es.actions.append(of.ofp_action_output(port = up_ports[0]))
            con.send(msg_es)
      
        down_ports = host_ports[edge_switch]
        pod_num = switch_pos[edge_switch][0]
        pos = switch_pos[edge_switch][1]
        prefix = eth_addr_format((pod_num << 8) + pos, 6) + ':'
        suffix = ':00:00'
        mask = 'ff:ff:ff:ff:00:00'
        msg_es = nx.nx_flow_mod(table_id = 2)
        msg_es.priority = 3000

        for port in down_ports:
            msg_es.match.eth_dst_with_mask = ( prefix + eth_addr_format(port, 2) + suffix, mask)
            msg_es.actions = [ of.ofp_action_output(port = port) ]
            con.send(msg_es)
                    

def toggle_agg_core_links():    
    for core_switch in core_switches:
        links = g[core_switch]
        #print 'core:{0}, links:{1}'.format(core, links)
        for src_port,link in links.iteritems():
            agg_link = g[link[0]][link[1]]
            agg_link[0] = -agg_link[0]
            #print 'link:{0}, agg_link:{1}'.format(link, agg_link) 


def bfs(root, pod):
    egs.append({})
    ags.append({})
    q = [root]
    pos = 0
    while len(q) != 0 :
        dpid = q.pop(0)
        if switch_pos[dpid][0] == -1 :#not visited
            if dpid in edge_switches:
                egs[pod][pos] = dpid
            elif dpid in agg_switches:
                ags[pod][pos] = dpid#prbly not use it
            switch_pos[dpid][0] = pod
            switch_pos[dpid][1] = pos
            pos += 1
            links = g[dpid]
            for src_port,link in links.iteritems():
                if ( (link[0] > 0) and (switch_pos[link[0]][0] == -1) ): #if not a core switch and a new switch then enque
                    q.append(link[0])


def _handle_PacketIn(event):
    global count
    log.debug("PacketIn : dpid:{0},port:{1},src:{2},dst:{3},dl_type:{4},count={5}".format(event.dpid, event.port, event.parsed.src.toStr(), event.parsed.dst.toStr(), hex(event.parsed.type), count ) )
    if event.port == of.OFPP_LOCAL:
        log.info('From OPENFLOW port')
        return
    if event.parsed.type == pkt.ethernet.ARP_TYPE:
        _process_arp_packet_in(event)
    count += 1


def _process_arp_packet_in(event):
    eth_pkt = event.parsed
    arp_pkt = event.parsed.payload
    log.info('ARP packet : type: {0}, dst ip:{1}, src ip:{2}, src mac:{3}, dst mac:{4}'.format(arp_pkt.opcode, arp_pkt.protodst.toStr(), arp_pkt.protosrc.toStr(), arp_pkt.hwsrc.toStr(), arp_pkt.hwdst.toStr()))
    org_mac = eth_pkt.src.toStr()
    ip = arp_pkt.protosrc.toStr()
    '''
    ip and mac known - normal arp or traditional vm migration or possible virtual ip - a went down, so b took over. a comes again and takes over. now ctlr knows a ip and mac, but when b took over, shal we remove a's mac from table. we shud right?
    ip known, mac unknown - probably virtual ip.
    arp probe check - ip is zero
    if ip not known, mac known - some host getting new ip. or like that vbox where one intf has many ips. we support only 1st case
    if both are not known, new host
    '''

    if ip in arp_table:#virtual ip or normal arp
        if(_check_handle_migration(event) == 0):
            _handle_arp(event)
    elif ip == zero_ip:
        log.info('ARP Probe detected')
        dst_ip = arp_pkt.protodst.toStr()
        if dst_ip in arp_table:
            _handle_arp(event)
        else:
            latent_host_pos[org_mac] = (event.dpid, event.port)
            #do edge bcast bcast NOTE : this is one place where real mac is leaking into the network. not really net, but to other hosts. but it shouldnt affect
            edge_bcast(eth_pkt.pack(), event.dpid, event.port)
    elif org_mac in actual_pmac:#one host getting a new ip and not using old one
        #in this case, just updating arp table should do i guess. translation can remain the same.
        arp_table[ip] = actual_pmac[org_mac]
        _handle_arp(event)
    else:
        pmac = _handle_new_host(org_mac, ip, event.dpid, event.port)
        handler_id = event.connection.addListenerByName("BarrierIn", _handle_BarrierIn_ARP)
        barrier = of.ofp_barrier_request()
        events[barrier.xid] = (handler_id, event, pmac, ip)
        event.connection.send(barrier)
 
def _handle_BarrierIn_ARP(e):
    if e.xid not in events:#not our barrier req
        return
    handler, event, pmac, ip = events.pop(e.xid)
    log.debug( 'Barrier received for pmac:{0}'.format(pmac) )
    e.connection.removeListener(handler)
    #update tables here
    arp_table[ip] = pmac
    org_mac = event.parsed.src.toStr()
    actual_pmac[org_mac] = pmac
    pmac_actual[pmac] = org_mac
    _handle_arp(event)


def _handle_arp(event):
    '''
    if its a req:check if we know the src mac. if we dont, its a new host so update table. 
    if you know the ip->pmac, respond. 
    if not, do a bcast. fr nw do flood on all edge switches. bad soln. have bcast semantics instead
    '''
    eth_pkt = event.parsed
    arp_pkt = event.parsed.payload
    org_mac = eth_pkt.src.toStr()
    if org_mac in actual_pmac:#wont be case for arp probe
        pmac_src = adrs.EthAddr(actual_pmac[org_mac])
    if arp_pkt.protodst.toStr() == arp_pkt.protosrc.toStr(): #its gratitous arp, nothing to do as new host would have already been detected
        return
    if arp_pkt.opcode == arp_pkt.REQUEST:
        dst_ip = arp_pkt.protodst.toStr()
        log.debug( 'ARP request : ip:{0}'.format(dst_ip) )
        if dst_ip in arp_table:
            #we know the mapping. respond back to inp port OFPP_IN_PORT.
            arp_reply = pkt.arp()
            arp_reply.hwsrc = adrs.EthAddr(arp_table[dst_ip])
            arp_reply.hwdst = eth_pkt.src
            arp_reply.opcode = pkt.arp.REPLY
            arp_reply.protosrc = arp_pkt.protodst
            arp_reply.protodst = arp_pkt.protosrc
            ether = pkt.ethernet()
            ether.type = pkt.ethernet.ARP_TYPE
            ether.dst = eth_pkt.src
            ether.src = arp_reply.hwsrc
            ether.payload = arp_reply
            msg = of.ofp_packet_out()
            #msg.in_port = event.port
            #msg.actions.append(of.ofp_action_output(port = of.OFPP_IN_PORT))
            msg.actions.append(of.ofp_action_output(port = event.port))
            msg.data = ether.pack()
            event.connection.send(msg)
        else:
            #bcast with src changed to sources pmac. also if its this switch, put inport as the it came from, so that it wont go to it
            log.debug( 'Broadcasting since we dont have ARP mapping for ip : {0}'.format(dst_ip) )
            arp_pkt.hwsrc = pmac_src
            eth_pkt.src = pmac_src
            #edge bcast:
            edge_bcast(eth_pkt.pack(), event.dpid, event.port)

    elif arp_pkt.opcode == arp_pkt.REPLY:
        #print 'ARP reply : src ip:{0}, dst ip:{1}'.format(arp_pkt.protosrc.toStr(), arp_pkt.protodst.toStr())
        arp_pkt.hwsrc = pmac_src
        eth_pkt.src = pmac_src
        if arp_pkt.protodst == zero_ip:#reply to arp probe
            msg = of.ofp_packet_out()
            dpid, prt = latent_host_pos[ eth_pkt.dst.toStr() ]
            msg.actions.append(of.ofp_action_output(port = prt ))
            msg.data = eth_pkt.pack()
            core.openflow.connections[dpid].send(msg)
            return
        arp_pkt.hwdst = adrs.EthAddr(pmac_actual[arp_pkt.hwdst.toStr()])
        dpid, port = pmac_pos(eth_pkt.dst.toStr())
        eth_pkt.dst = arp_pkt.hwdst
        msg = of.ofp_packet_out()
        #msg.actions.append(of.ofp_action_output(port = of.OFPP_TABLE))
        msg.actions.append(of.ofp_action_output(port = port))
        msg.data = eth_pkt.pack()
        core.openflow.connections[dpid].send(msg)


def edge_bcast(data, src_dpid, src_port):
    msg = of.ofp_packet_out()#msg for other edge switches
    msg.data = data
    for es in edge_switches:
        msg.actions = [ of.ofp_action_output(port = hp) for hp in host_ports[es] ] 
        if es == src_dpid:
            msg.actions.pop( host_ports[es].index(src_port) )
        core.openflow.connections[es].send(msg)

def _handle_new_host(org_mac, ip, dpid, port):
    con = core.openflow.connections[dpid]
    pmac = _assign_pmac(dpid, port, org_mac)
    #i guess nothing more to do here. in multi table, the switch would have sent here and replied to the host.
    msg = nx.nx_flow_mod(table_id = 0)
    msg.priority = 5000
    msg.match.NXM_OF_IN_PORT = port
    msg.match.eth_src = org_mac
    msg.actions.append(of.ofp_action_dl_addr.set_src(adrs.EthAddr(pmac)))
    msg.actions.append(nx.nx_action_resubmit.resubmit_table(table=1))
    con.send(msg)
    msg = nx.nx_flow_mod(table_id = 1)
    msg.priority = 5000
    msg.match.eth_dst = pmac
    msg.actions.append(of.ofp_action_dl_addr.set_dst(adrs.EthAddr(org_mac)))
    msg.actions.append(of.ofp_action_output(port = port))
    con.send(msg)
    return pmac


def _assign_pmac(edge_dpid, port, org_mac):
    '''assumes the dpid passed is that of an edge switch'''
    prefix = (long(switch_pos[edge_dpid][0])<<32) + (switch_pos[edge_dpid][1]<<24) + (port << 16)
    vmid = assigned_pmac[edge_dpid][port].index('0')#raise exception if all are assigned
    assigned_pmac[edge_dpid][port] = assigned_pmac[edge_dpid][port][:vmid] + '1' + assigned_pmac[edge_dpid][port][vmid + 1:]
    #print 'prefix = ' + str(prefix) + 'vmid:' + str(vmid)
    pmac_long = prefix + vmid
    pmac = eth_addr_format(pmac_long)
    return pmac


def pmac_pos(pmac):
    '''returns the dpid this pmac belongs to and the port(0 index)'''
    tmp = pmac.replace(':','')
    pod = int(tmp[:4], 16)
    pos = int(tmp[4:6], 16)
    port = int(tmp[6:8], 16)
    return egs[pod][pos], port


def _check_handle_migration(event):
    '''returns 0 for no migration. returns 1 for migration'''
    eth_pkt = event.parsed
    arp_pkt = event.parsed.payload
    org_mac = eth_pkt.src.toStr() #actual mac of the current machine. same as old amac if vm mig. if vip, then diff.
    src_ip = arp_pkt.protosrc.toStr()
    old_pmac = arp_table[src_ip]
    old_amac = pmac_actual[old_pmac] #
    sw, port = pmac_pos(old_pmac)
    #print 'In migration check. IP:{0}, old pos - {1}:{2} new pos - {3}:{4}'.format(src_ip, sw, port, event.dpid, event.port)
    if (sw != event.dpid) or (port != event.port):
        '''
        this host has migrated or virtual ip
        assign new pmac.
        add trans in new switch.
        add entry in old switch to frwrd to some agg switch replacing old pmac with new pmac
        remove prev trans tabbles from old switch.
        update our internal tables
        change the assigned mac string - not assign the old pmac for the timeout period of time
        '''
        if org_mac == old_amac:
            log.info( 'VM migration detected. IP:{0}, old pos - {1}:{2} new pos - {3}:{4}'.format(src_ip, sw, port, event.dpid, event.port) )
        else:
            log.info( 'Virtual ip takeover detected. IP:{0}, old pos - {1}:{2} new pos - {3}:{4}'.format(src_ip, sw, port, event.dpid, event.port) )

        return move_host(src_ip, event.dpid, event.port, org_mac, event)
        new_pmac = _handle_new_host(org_mac, src_ip, event.dpid, event.port)
        barrier = of.ofp_barrier_request()
        event.connection.send(barrier)
        
        old_switch_con = core.openflow.connections[sw]
        msg = nx.nx_flow_mod(table_id = 0)
        msg.priority = 8000
        msg.hard_timeout = arp_cache_timeout
        msg.match.eth_dst = old_pmac
        rewrite_action = of.ofp_action_dl_addr.set_dst(adrs.EthAddr(new_pmac))
        msg.actions.append(rewrite_action)
        #choose one up link by hashing
        up_ports = g[event.dpid].keys()
        num_routes = len(up_ports)
        #msg.actions.append(of.ofp_action_output(port = up_ports[ random.randint(0, num_routes - 1) ] ))#simple hashing. edge switch x sends to agg switch x
        #old_switch_con.send(msg)


        #if more than 2 up ports are there, then hashing is req. if 1 up port, then no hashing. if 2, then the other port other than inp port
        if num_routes == 1: #only one route
            msg.actions = [ rewrite_action, of.ofp_action_output(port = of.OFPP_IN_PORT) ]
            old_switch_con.send(msg)
        elif num_routes == 2:
            msg.match.NXM_OF_IN_PORT = up_ports[0]
            msg.actions = [ rewrite_action, of.ofp_action_output(port = up_ports[1] ) ]
            old_switch_con.send(msg)
            msg.match.NXM_OF_IN_PORT = up_ports[1]
            msg.actions = [ rewrite_action, of.ofp_action_output(port = up_ports[0] ) ]
            old_switch_con.send(msg)
        else: #3 or more ports. here need hahsing. based on src port num. also avoid the inp port. so match based on that also
            num_routes -= 1 #since we avoid each inp port
            num_bits  = int(math.floor(math.log(num_routes, 2)))
            x = 2 ** num_bits   #Assumption:one edge switch doesn't have more than 255 direct connections to agg switches
            mask = '00:00:00:' + eth_addr_format(x - 1, 2) + ':00:00'
            prefix = '00:00:00:'
            suffix = ':00:00'
            for ii in range(num_routes + 1):
                cand_ports = list(up_ports)
                msg.match.NXM_OF_IN_PORT = cand_ports.pop(ii) #remove the inp port from the list of op ports
                for i in range(num_routes):
                    port = i % x
                    msg.match.eth_src_with_mask = ( prefix + eth_addr_format(port, 2) + suffix, mask)
                    msg.actions = [ rewrite_action, of.ofp_action_output(port = cand_ports[i]) ]
                    old_switch_con.send(msg)

        msg = nx.nx_flow_mod(table_id = 0, command=of.OFPFC_DELETE)
        msg.priority = 5000
        msg.match.NXM_OF_IN_PORT = port
        msg.match.eth_src = old_amac
        old_switch_con.send(msg)
        msg = nx.nx_flow_mod(table_id = 1, command=of.OFPFC_DELETE)
        msg.priority = 5000
        msg.match.eth_dst = old_pmac
        old_switch_con.send(msg)
        barrier = of.ofp_barrier_request()
        old_switch_con.send(barrier)
        
        arp_table[src_ip] = new_pmac
        actual_pmac.pop( pmac_actual.pop(old_pmac) )#remove old pmac to actual and vice versa since they are not valid anymore
        actual_pmac[org_mac] = new_pmac
        pmac_actual[new_pmac] = org_mac
        #this nxt 2 lines should be in a fn called after the timeout. we dont want to assing old pmac to anyone until then
        def _remove_old_pmac():
            vmid = int(old_pmac[-5:].replace(':',''),16)
            assigned_pmac[sw][port] = assigned_pmac[sw][port][:vmid] + '0' + assigned_pmac[sw][port][vmid + 1:]
        Timer(arp_cache_timeout, _remove_old_pmac)
        return 1
    return 0

def move_host(ip, new_sw, new_port, mac=None, event=None):
    '''
    move the known host with the given ip from its current pos to the given edge switch and port. mac is the original mac of the host ot be moved. needed in case of vip since 2 macs are involved and the entries should be removed. event is in case vm has migrated, then process the arp for this event since barrier are used.
    returns 0 if nothign was done. 1 if it was migrated
    '''
    if ip not in arp_table:
        log.fatal('IP-{0} is not known'.format(ip))
        return 0
    old_pmac = arp_table[ip]
    old_amac = pmac_actual[old_pmac] #used for del old entries in prev sw
    org_mac = mac if mac else old_amac #the current mac of the src machine. same incase of vm mig. diff incase of vip
    old_sw, old_port = pmac_pos(old_pmac)
    if (old_sw == new_sw) and (old_port == new_port):
        return 0 #nothing to do. old pos and new pos are same
    new_sw_con = core.openflow.connections[new_sw]
    new_pmac = _handle_new_host(org_mac, ip, new_sw, new_port)
    #prbly need to wait here.
    handler_id = [0,0]
    barrier_ns = of.ofp_barrier_request()
    xid_ns = barrier_ns.xid
    def _handle_BarrierIn_Mig_NS(e):
        #barrier handler for new switch
        if e.xid != xid_ns: #if its not this barrier, ignore
            return
        #log.debug( 'Barrier received for migrated pmac:{0}'.format(new_pmac) )
        e.connection.removeListener(handler_id[0])

        old_sw_con = core.openflow.connections[old_sw]
        msg = nx.nx_flow_mod(table_id = 0)
        msg.priority = 8000
        msg.hard_timeout = arp_cache_timeout
        msg.match.eth_dst = old_pmac
        rewrite_action = of.ofp_action_dl_addr.set_dst(adrs.EthAddr(new_pmac))
        msg.actions.append(rewrite_action)
        #choose one up link by hashing
        up_ports = g[old_sw].keys()
        num_routes = len(up_ports)
        #msg.actions.append(of.ofp_action_output(port = up_ports[ random.randint(0, num_routes - 1) ] ))#simple hashing. edge switch x sends to agg switch x
        #old_sw_con.send(msg)

        #if more than 2 up ports are there, then hashing is req. if 1 up port, then no hashing. if 2, then the other port other than inp port
        if num_routes == 1: #only one route
            msg.actions = [ rewrite_action, of.ofp_action_output(port = of.OFPP_IN_PORT) ]
            old_sw_con.send(msg)
        elif num_routes == 2:
            msg.match.NXM_OF_IN_PORT = up_ports[0]
            msg.actions = [ rewrite_action, of.ofp_action_output(port = up_ports[1] ) ]
            old_sw_con.send(msg)
            msg.match.NXM_OF_IN_PORT = up_ports[1]
            msg.actions = [ rewrite_action, of.ofp_action_output(port = up_ports[0] ) ]
            old_sw_con.send(msg)
        else: #3 or more ports. here need hahsing. based on src port num. also avoid the inp port. so match based on that also
            num_routes -= 1 #since we avoid each inp port
            num_bits  = int(math.floor(math.log(num_routes, 2)))
            x = 2 ** num_bits   #Assumption:one edge switch doesn't have more than 255 direct connections to agg switches
            mask = '00:00:00:' + eth_addr_format(x - 1, 2) + ':00:00'
            prefix = '00:00:00:'
            suffix = ':00:00'
            for ii in range(num_routes + 1):
                cand_ports = list(up_ports)
                msg.match.NXM_OF_IN_PORT = cand_ports.pop(ii) #remove the inp port from the list of op ports
                for i in range(num_routes):
                    port = i % x
                    msg.match.eth_src_with_mask = ( prefix + eth_addr_format(port, 2) + suffix, mask)
                    msg.actions = [ rewrite_action, of.ofp_action_output(port = cand_ports[i]) ]
                    old_sw_con.send(msg)

        msg = nx.nx_flow_mod(table_id = 0, command=of.OFPFC_DELETE)
        msg.priority = 5000
        msg.match.NXM_OF_IN_PORT = old_port
        msg.match.eth_src = old_amac
        old_sw_con.send(msg)
        msg = nx.nx_flow_mod(table_id = 1, command=of.OFPFC_DELETE)
        msg.priority = 5000
        msg.match.eth_dst = old_pmac
        old_sw_con.send(msg)
        
        #maybe send barrier and wait? to make sure its processed so that pkts to old pmac can still reach new place?
        barrier_os = of.ofp_barrier_request()
        xid_os = barrier_os.xid
        def _handle_BarrierIn_Mig_OS(e):
            #barrier handler for old switch
            if e.xid != xid_os: #if its not this barrier, ignore
                return
            #log.debug( 'Barrier received for diverting flows to pmac:{0}'.format(new_pmac) )
            e.connection.removeListener(handler_id[1])
            arp_table[ip] = new_pmac
            actual_pmac.pop( pmac_actual.pop(old_pmac) )#remove old pmac to actual and vice versa since they are not valid anymore
            actual_pmac[org_mac] = new_pmac
            pmac_actual[new_pmac] = org_mac
            #this nxt 2 lines should be in a fn called after the timeout. we dont want to assing old pmac to anyone until then
            def _remove_old_pmac():
                vmid = int(old_pmac[-5:].replace(':',''),16)
                assigned_pmac[old_sw][old_port] = assigned_pmac[old_sw][old_port][:vmid] + '0' + assigned_pmac[old_sw][old_port][vmid + 1:]
            Timer(arp_cache_timeout, _remove_old_pmac)
            if event:
                _handle_arp(event)
        handler_id[1] = old_sw_con.addListenerByName("BarrierIn", _handle_BarrierIn_Mig_OS)
        old_sw_con.send(barrier_os)
    handler_id[0] = new_sw_con.addListenerByName("BarrierIn", _handle_BarrierIn_Mig_NS)
    new_sw_con.send(barrier_ns)
    log.info('IP:{0} moved to edge swith:{1} port:{2}'.format(ip, new_sw, new_port))
    return 1

def move_batch(file_name):
    inp_file = open(file_name, 'r')
    line_num = 0
    num_moved = 0
    for line in inp_file:
        line_num += 1
        params = line.strip().split()
        if len(params) != 3:
            log.fatal( 'Invalid number of parameters. Skipping line {0} : "{1}"'.format(line_num, line.strip()) )
            continue
        success = move_host(params[0], int(params[1]), int(params[2]))
        num_moved += success
    inp_file.close()
    log.info( 'Batch move done. Successfully moved {0} hosts'.format(num_moved) )

def eth_addr_format(n, l = 12):
    s = hex(n)[2:]
    if s[-1] == 'L':
        s = s[:-1]
    s = ('0' * (l - len(s) )) + s
    res = ''
    for i in range(0, l-2, 2):
        res = res + s[i:i+2] + ':'
    res = res + s[-2:]
    return res

def launch(setup = 0, num_links = 0, max_link_interval = 0, connected_perc = 50, arp_timeout = 2):
    global setup_time
    global total_num_links
    global link_timeout
    global max_connected_perc
    global arp_cache_timeout
    setup_time = int(setup)
    total_num_links = 2 * int(num_links)
    link_timeout = int(max_link_interval)
    max_connected_perc = float(connected_perc)/100.0
    arp_cache_timeout = int(arp_timeout)
    if total_num_links:
        log.info( 'Num of links to wait for : {0}'.format(total_num_links) )
    if setup_time:
        log.info( 'Setup time : {0}'.format(setup_time) )
    if not total_num_links and not setup_time and not link_timeout:
        link_timeout = 5 #defaulting to link timeout of 10s
    if link_timeout:
        log.info( 'Max interval between two link discovery events : {0}'.format(link_timeout) )
    log.info( 'Max fraction of links to switches in an edge switch : {0}'.format(max_connected_perc) )
    log.info( 'Max arp cache timeout of a host : {0}'.format(arp_cache_timeout) )
    core.openflow.addListenerByName("ConnectionUp", _handle_ConnectionUp)
    core.openflow_discovery.addListenerByName("LinkEvent", _handle_LinkEvent)
    core.call_when_ready(_ready, "openflow_discovery")
    core.Interactive.variables['move'] = move_host
    core.Interactive.variables['move_batch'] = move_batch

