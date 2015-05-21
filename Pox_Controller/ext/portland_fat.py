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
from pox.lib.recoco import Sleep
import time
import datetime

log = core.getLogger("****Portland****")

num_pods = 4
half_num_pods = 2
num_switches = 0
num_links = 0
total_switches = 0
total_links = 0

all_switches = set()
edge_switches = set()
agg_switches = set()
core_switches = set()


barrier_recvd = False


cs = []
ags = []
egs = []

switch_pos = {}
assigned_pmac = {}

g = {}

tmp = []
events = {}

arp_table = {} #ip->pmac
pmac_actual = {}
actual_pmac = {}

init_asgn_str = '1' + ('0' * 254) + '1'

count  = 0

'''
1st listen fr connection up events. keep a count. dont start unitl (5/4)k sq switches are connected
and k cube/2 links up. or double that since im getting 2 link up for every link


Graph is stored as a dictionay. Key is dpid. Value is a list of links.
the link pos in list is the src port num, link is a list of size 2. the dpid and port num of dest switch

ASSUMPTION : mininet creates the port no like we want.
1st half ports of AS is con to CS. next half to all ES in order
1st half of all ES is connectd to AS and next half to hosts in order

First fig out whihc is an edge switch and agg and core.
Then follow the 1st port of any core switch since we dont know which is the 1st core switch and so on. That has to be AS pod 1.
Now add all ithe switches form (k/2 + 1)th port to kth port of that AS as ES of pod1.

For every edge host, initialize k/2 strings of 2^16 zeros. This is the assigned list for pmac mgmt. Lets keep last an first as 1, we wont give these addresses or instead start pod num from 1
On new hosts, make a one after assigning that pmac to the new host. On vm migration, amke that pmac zero after timeout

pmac : pod_num, switch_num, port_num start from 0
but port_num in switch starts from 1

We have to have multiple table support. Coz we need to rewrite the packet with pmac and then do routing. Routing is general


change all prints to log.
'''



def _handle_ConnectionUp (event):
    #When a switch connects, create a new node in graph
    global num_switches
    global g
    num_switches += 1
    g[event.dpid] = [ [-1,-1] for i in range(num_pods)]
    all_switches.add(event.dpid)
    # Turn on ability to specify table in flow_mods
    msg = nx.nx_flow_mod_table_id()
    event.connection.send(msg)
    #del_tables(event.connection) #if u del tables, even the lldp flow goes off. so dont del tables or u install that flow. i think when it connects to a ctlr, tbales are automaticlly being cleared anyway or pox is doing. bt its being del

def del_tables(con):
    msg = nx.nx_flow_mod(table_id = 0, command=of.OFPFC_DELETE)
    con.send(msg)
    msg = nx.nx_flow_mod(table_id = 1, command=of.OFPFC_DELETE)
    con.send(msg)
    msg = nx.nx_flow_mod(table_id = 2, command=of.OFPFC_DELETE)
    con.send(msg)


def _handle_LinkEvent (event):
    # When links change, update graph
    global g
    global num_links
    link = event.link
    try:
        if(event.added):#new link. assume symmetric graph and add 2 links?
            g[link.dpid1][link.port1 - 1] = [link.dpid2, link.port2 - 1]
            num_links += 1
        else:
            g[link.dpid1][link.port1 - 1] = [-1, -1]
            num_links -= 1
    except:        
            log.fatal('ling update failed for link : ' + str(link))


def _check():
    if (num_switches != total_switches) | (num_links != total_links):
        Timer(2, _check)
    else:
        log.info('******************Starting Portland Identification**************')
        _start()

def _start():
    for dpid,links in g.iteritems():
        if(links[num_pods - 1][0] == -1):#if the last port is not connected to a switch, edge port
            edge_switches.add(dpid)
            #create assigned list
            assigned_pmac[dpid] = {}
            for i in range(half_num_pods, num_pods):#for all the edge host ports, seems like a lot of mem. use something like a bit set or somehting
                assigned_pmac[dpid][i] = '1' + ('0' * 254) + '1'
    for dpid,links in g.iteritems():
        if(links[num_pods - 1][0] in edge_switches):#if the last port is not connected to an edge switch, this is an agg switch
            agg_switches.add(dpid)
    core_switches = all_switches.difference(agg_switches.union(edge_switches))
    a_core_switch = core_switches.pop()
    core_switches.add(a_core_switch)
    print edge_switches
    print agg_switches
    print core_switches

    for pod_num in range(num_pods):#each link in core switch('s' here) corresponds to one pod
        agg = g[a_core_switch][pod_num][0]
        egs.append([])
        for i in range(half_num_pods):#the links of upper half ports are edge switches
            dpid = g[agg][half_num_pods + i][0]
            egs[pod_num].append(dpid)
            switch_pos[dpid] = [pod_num, i]
        aes = egs[pod_num][0]
        ags.append([])
        for i in range(half_num_pods):
            dpid = g[aes][i][0]
            ags[pod_num].append(dpid)
            switch_pos[dpid] = [pod_num, i]
    for agg in ags[0]:
        for i in range(half_num_pods):
            dpid = g[agg][i][0]
            cs.append(g[agg][i][0])
            switch_pos[dpid] = [-1, i]
    print cs
    print ags
    print egs
    print switch_pos
    '''
    insert flows table entries
    put all frwrding in table 1 and replacing macs with pmac in table 0
    todo : replacing macs, routing, bcast semantics 
    routing : for longer matches, give higher priority. of might not do longest prefix match. so force using priority
    for all edge switches, for upward packets, sending arp req(dest is bcast) to ctlr, v high priority
        for arp reply(dest not bcast), send to ctlr and submit to table 1 whihc does routing
    
    for ags, for other pods, hash and send to a cs. for same pod, look at switch pos and send to k/2+pos    
    for all core switches, have k entries. mask is 1st 16 bits. lopp from 0 to k-1, in 1st 16 bits, send to port k. amd maybe fr bcast adrs, flood
    '''
    
    
    for pod_num in range(num_pods):
        msg_cs = nx.nx_flow_mod(table_id = 0)
        msg_cs.match.eth_dst_with_mask = ( eth_addr_format(pod_num, 4) + ":00:00:00:00", "ff:ff:00:00:00:00")
        msg_cs.actions.append(of.ofp_action_output(port = pod_num + 1))
        for core_switch in cs:
            core.openflow.connections[core_switch].send(msg_cs)
        msg_a = nx.nx_flow_mod(table_id = 0)
        msg_a.match.eth_dst_with_mask = ( eth_addr_format(pod_num, 4) + ":00:00:00:00", "ff:ff:00:00:00:00")
        msg_a.priority = 2000
        msg_a.actions.append(of.ofp_action_output(port = (pod_num/2) + 1))#simple hashing
        msg_e = nx.nx_flow_mod(table_id = 2)
        msg_e.match.eth_dst_with_mask = ( eth_addr_format(pod_num, 4) + ":00:00:00:00", "ff:ff:00:00:00:00")
        msg_e.priority = 2000
        msg_e.actions.append(of.ofp_action_output(port = (pod_num/2) + 1))#simple hashing
        #msg_e.actions.append(nx.nx_action_resubmit.resubmit_table(table=1))
        for i in range(half_num_pods):
            for pod in range(num_pods):#put the above entry into all the agg and edge switches of all pods
                #print 'pod_num:{0}, i:{1}, match:{2}, ags:{3}, egs:{4}'.format(pod_num, i, msg.match, ags[pod_num][i], egs[pod_num][i] )
                core.openflow.connections[ags[pod][i]].send(msg_a)
                core.openflow.connections[egs[pod][i]].send(msg_e)
            msg_as = nx.nx_flow_mod(table_id = 0)
            prefix = eth_addr_format((pod_num<<8) + i, 6)
            msg_as.match.eth_dst_with_mask = ( prefix + ":00:00:00", "ff:ff:ff:00:00:00")
            msg_as.priority = 3000
            msg_as.actions.append(of.ofp_action_output(port = half_num_pods + i + 1))
            for j in range(half_num_pods):
                core.openflow.connections[ags[pod_num][j]].send(msg_as)
                #check for port num
                msg_es = nx.nx_flow_mod(table_id = 2)
                msg_es.match.eth_dst_with_mask = ( prefix + ':' + eth_addr_format(half_num_pods + j, 2) + ":00:00", "ff:ff:ff:ff:00:00")
                msg_es.priority = 3000
                msg_es.actions.append(of.ofp_action_output(port = half_num_pods + j + 1))
                #msg_es.actions.append(nx.nx_action_resubmit.resubmit_table(table=1))
                core.openflow.connections[egs[pod_num][i]].send(msg_es)

    for es in edge_switches:#for arp entries and wildcard resubmissions
        msg = nx.nx_flow_mod()
        msg.priority = 9000
        msg.match.eth_type = pkt.ethernet.ARP_TYPE
        msg.actions.append(of.ofp_action_output(port = of.OFPP_CONTROLLER))
        for i in range(half_num_pods + 1, num_pods + 1):
            msg.match.NXM_OF_IN_PORT = i
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
    down_flood = []
    up_flood = []
    for port in range(1, half_num_pods + 1):#flood to all host ports
        up_flood.append(of.ofp_action_output(port = port))
        down_flood.append(of.ofp_action_output(port = port + half_num_pods))

    #trickle down
    msg = nx.nx_flow_mod(table_id = 0)
    msg.priority = 8500
    msg.match.eth_dst = 'ff:ff:ff:ff:ff:ff'
    for pod_num in range(num_pods):
        for i in range(half_num_pods):
            cona = core.openflow.connections[ags[pod_num][i]]
            cone = core.openflow.connections[egs[pod_num][i]]
            for in_port in range(1, half_num_pods + 1):
                #from up link, trickle down
                msg.actions = list(down_flood)
                msg.match.NXM_OF_IN_PORT = in_port
                cona.send(msg)
                cone.send(msg)
                #from down link, flood down and choose one up
                msg.match.NXM_OF_IN_PORT = in_port + half_num_pods
                msg.actions.pop(in_port  - 1)#remove the port it came from
                msg.actions.append(of.ofp_action_output(port = i + 1))#simple hashing to pick one up link.
                cona.send(msg)
                cone.send(msg)

    msg.actions = [of.ofp_action_output(port = of.OFPP_FLOOD)]
    for core_sw in cs:
        core.openflow.connections[core_sw].send(msg)

    #handle packet in after flow tables are installed and everything is set up
    core.openflow.addListenerByName("PacketIn", _handle_PacketIn)
    

def _handle_PacketIn(event):
    global count
    print "PacketIn : dpid:{0},port:{1},src:{2},dst:{3},dl_type:{4},count={5}".format(event.dpid, event.port, event.parsed.src.toStr(), event.parsed.dst.toStr(), hex(event.parsed.type), count )
    tmp.append(event)
    if(event.port > num_pods ):
        print 'From OPENFLOW port'
        return
    if event.parsed.type == pkt.ethernet.ARP_TYPE:
        _process_arp_packet_in(event)
    count += 1


def _process_arp_packet_in(event):    
    '''
    if its an arp msg(note, arp src hw adrs is the org eth hw adrs) assuming its from edge host, if its a req:check if we know the src mac. if we dont, its a new host so update table. 
    if you know the ip->pmac, respond. 
    if not, do a bcast. fr nw do flood on all edge switches. bad soln. have bcast semantics instead
    '''
    eth_pkt = event.parsed
    arp_pkt = event.parsed.payload
    org_mac = eth_pkt.src.toStr()
    if org_mac not in actual_pmac:#new host. assign pmac, insert flow table in switch and add to table, mean we dont have arp entry also fr this
        ip = arp_pkt.protosrc.toStr()
        pmac = _handle_new_host(org_mac, ip, event.dpid, event.port)
        handler_id = event.connection.addListenerByName("BarrierIn", _handle_BarrierIn)
        barrier = of.ofp_barrier_request()
        events[barrier.xid] = (handler_id, event, pmac, ip)
        event.connection.send(barrier)
    else:
        _handle_arp(event)
 
def _handle_BarrierIn(e):
    if e.xid not in events:#not our barrier req
        return
    handler, event, pmac, ip = events.pop(e.xid)
    e.connection.removeListener(handler)
    #update tables here
    arp_table[ip] = pmac
    org_mac = event.parsed.src.toStr()
    actual_pmac[org_mac] = pmac
    pmac_actual[pmac] = org_mac
    _handle_arp(event)
    
def _handle_arp(event):
    eth_pkt = event.parsed
    arp_pkt = event.parsed.payload
    org_mac = eth_pkt.src.toStr()
    pmac_src = adrs.EthAddr(actual_pmac[org_mac]) 
    #gratituos arp is when the src and dst ip in arp are same. check it and update tables in case of new host. or else return
    if arp_pkt.opcode == arp_pkt.REQUEST:
        dst_ip = arp_pkt.protodst.toStr()
        #print 'ARP request : ip:{0}'.format(dst_ip)
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
            #print 'Broadcasting since we dont have ARP mapping'
            arp_pkt.hwsrc = pmac_src
            eth_pkt.src = pmac_src
            #edge bcast:
            msg = of.ofp_packet_out()
            for i in range(half_num_pods + 1, num_pods + 1):#pkt out to host ports
                msg.actions.append(of.ofp_action_output(port = i))
            msg.data = eth_pkt.pack()
            other_switches = edge_switches - set([event.dpid])
            for es in other_switches:
                core.openflow.connections[es].send(msg) 
            msg.actions.pop(event.port - half_num_pods - 1)
            event.connection.send(msg)  

    elif arp_pkt.opcode == arp_pkt.REPLY:
        #print 'ARP reply : src ip:{0}, dst ip:{1}'.format(arp_pkt.protosrc.toStr(), arp_pkt.protodst.toStr())
        arp_pkt.hwdst = adrs.EthAddr(pmac_actual[arp_pkt.hwdst.toStr()])
        arp_pkt.hwsrc = pmac_src
        eth_pkt.src = pmac_src
        msg = of.ofp_packet_out()
        msg.actions.append(of.ofp_action_output(port = of.OFPP_TABLE))
        msg.data = eth_pkt.pack()
        core.openflow.connections[cs[0]].send(msg)


def pmac_pos(pmac):
    '''returns the dpid this pmac belongs to and the port(0 index)'''
    tmp = pmac.replace(':','')
    pod = int(tmp[:4], 16)
    pos = int(tmp[4:6], 16)
    port = int(tmp[6:8], 16)
    return egs[pod][pos], port

def _handle_new_host(org_mac, ip, dpid, port):
    con = core.openflow.connections[dpid]
    pmac = _assign_pmac(dpid, port - 1, org_mac)
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


def _handle_migration(event):
    eth_pkt = event.parsed
    arp_pkt = event.parsed.payload
    org_mac = eth_pkt.src.toStr()
    dst_ip = arp_pkt.protodst.toStr()
    #old_pmac = arp_table[dst_ip]
    old_pmac = actual_pmac[org_mac]
    sw, port = pmac_pos(old_pmac)
    if (sw != event.dpid) or (port+1 != event.port):
        '''
        this host has migrated.
        assign new pmac.
        add trans in new switch.
        remove prev trans tabbles from old switch.
        add entry in old switch to frwrd to some agg switch replacing old pmac with new pmac
        update our internal tables
        change the assigned mac string - not assign the old pmac for the timeout period of time
        '''
        new_pmac = _handle_new_host(org_mac, dst_ip, event.dpid, event.port)
        msg = nx.nx_flow_mod(table_id = 0, command=of.OFPFC_DELETE)
        msg.priority = 5000
        msg.match.eth_src = org_mac
        event.connection.send(msg)
        msg = nx.nx_flow_mod(table_id = 1, command=of.OFPFC_DELETE)
        msg.priority = 5000
        msg.match.eth_dst = old_pmac
        event.connection.send(msg)
        
        msg = nx.nx_flow_mod(table_id = 0)
        msg.priority = 8000
        msg.hard_timeout = arp_timeout
        msg.match.eth_dst = old_pmac
        msg.actions.append(of.ofp_action_dl_addr.set_dst(adrs.EthAddr(new_pmac)))
        msg.actions.append(of.ofp_action_output(port = switch_pos(event.dpid)[1] + 1 ))#simple hashing. edge switch x sends to agg switch x
        event.connection.send(msg)
        
        arp_table[dst_ip] = new_pmac
        actual_pmac.pop(pmac_actual[old_pmac])
        pmac_actual.pop(old_pmac)
        #this nxt 2 lines should be in a fn called after the timeout. we dont want to assing old pmac to anyone until then
        vmid = int(s[-5:].replace(':',''),16)
        assigned_pmac[event.dpid][event.port] = assigned_pmac[event.dpid][event.port][:vmid] + '0' + assigned_pmac[event.dpid][event.port][vmid + 1:]

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

def launch(k = 4, arpTimeout = 60):
    global num_pods
    global half_num_pods
    global total_switches
    global total_links
    global arp_timeout
    num_pods = k
    half_num_pods = num_pods / 2
    total_switches = 5 * ((k ** 2) / 4)
    total_links = k ** 3 # twice the actual links
    arp_timeout = arpTimeout
    core.openflow.addListenerByName("ConnectionUp", _handle_ConnectionUp)
    core.openflow_discovery.addListenerByName("LinkEvent", _handle_LinkEvent)
    core.call_when_ready(_check, "openflow_discovery")


