# Copyright (c) 2016 Qosmos
# All Rights Reserved
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import etcd
import json
import vpp_papi
import time
from networking_vpp import feature
from networking_vpp.agent.server import LEADIN
import networking_vpp.agent.vpp

from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class VPPInterfaceEx(networking_vpp.agent.vpp.VPPInterface):

    def create_loopback(self, mac=""):
        self.LOG.debug("Creating local interface")
        t = self._vpp.create_loopback(mac_address=mac)
        self._check_retval(t)
        return t.sw_if_index

    def delete_loopback(self, idx):
        self.LOG.debug("Deleting VPP interface - index: %s" % idx)
        t = self._vpp.delete_loopback(sw_if_index=idx)
        self._check_retval(t)

    def enable_port_mirroring(self, source_idx, dest_idx, direction):
        self.LOG.debug("Enable span from %d to %d",
                       source_idx, dest_idx)
        if direction == 'IN':
            state = 1
        elif direction == 'OUT':
            state = 2
        else:
            state = 3
        t = self._vpp.sw_interface_span_enable_disable(sw_if_index_from=source_idx,
                                                       sw_if_index_to=dest_idx,
                                                       state=state)
        self._check_retval(t)

    def disable_port_mirroring(self, source_idx, dest_idx):
        self.LOG.debug("Disable span from %d to %d",
                       source_idx, dest_idx)
        t = self._vpp.sw_interface_span_enable_disable(sw_if_index_from=source_idx,
                                                       sw_if_index_to=dest_idx,
                                                       state=0)
        self._check_retval(t)

    def dump_port_mirroring(self):
        r = self._vpp.sw_interface_span_dump()
        self.LOG.info(str(r))

    def bridge_enable_broadcast(self, bridge_domain_id):
        L2_LEARN = (1<<0)
        L2_FWD = (1<<1)
        L2_FLOOD =  (1<<2)
        L2_UU_FLOOD = (1<<3)
        L2_ARP_TERM = (1<<4)

        self.LOG.debug("Enable broadcase (disable mac learning) for bridge %d",
                       bridge_domain_id)
        t = self._vpp.bridge_flags(bd_id=bridge_domain_id, is_set=0,
                                   feature_bitmap=(L2_LEARN | L2_FWD |
                                                   L2_FLOOD | L2_UU_FLOOD |
                                                   L2_ARP_TERM))
        t = self._vpp.bridge_flags(bd_id=bridge_domain_id, is_set=1,
                                   feature_bitmap=L2_FLOOD)
        self._check_retval(t)


class TaasServiceAgentFeature(feature.AgentFeature):
    path = 'taas_service'

    def __init__(self, conf, etcd_client, vppf):
        super(TaasServiceAgentFeature, self).__init__(conf, etcd_client, vppf)
        # is this legal ?
        # /!\ We can NOT create an other instance of VPPInterface
        # as vpp_papi can only be connected once per process !
        self.vppf.vpp.__class__ = VPPInterfaceEx
        self.tap_services = {}

    def key_set(self, key, value):
        # Create or update == bind
        tap_service_id = key
        data = json.loads(value)
        port_uuid = str(data['tap_service']['port_id'])

        # TODO(cfontaine): read VPP current configuration to get
        # the port_sw_if_idx with the tag. Until this information is
        # available, read from etcd even if it is not reliable on resync case.

        port_sw_if_idx = self.vppf.vpp.get_interface_by_tag(port_uuid)
        for (bd_id, ifaces) in self.vppf.vpp.get_ifaces_in_bridge_domains():
            if port_sw_if_idx in ifaces :
                old_bridge_domain_id = bd_id
                break
        else:
            old_bridge_domain_id = 0

        bridge_data = self.vppf.network_on_host(physnet, network_type, data['taas_id'])
        self.vppf.vpp.bridge_enable_broadcast(bridge_data['bridge_domain_id'])
        self.vppf.vpp.add_to_bridge(bridge_data['bridge_domain_id'], port_sw_if_idx)

        # TODO(cfontaine): check real state before reporting everything is OK
        data['tap_service']['status'] = 'ACTIVE'
        oper_data = {'service_bridge': bridge_data,
                     'port': {'iface_idx': port_sw_if_idx,
                              'bridge_domain_id': old_bridge_domain_id},
                     'tap_service': data['tap_service']}

        self._etcd_write(self._operational_key_space(self.host) +
                                '/%s' % tap_service_id,
                                json.dumps(oper_data))

    def key_delete(self, key, value):
        # Removing key == desire to unbind
        # rebind iface to appropriate bridge
        taas_path = self._operational_key_space(self.host) + '/' + key
        tap_service_info = json.loads(self.etcd_client.read(taas_path).value)
        self.vppf.vpp.add_to_bridge(tap_service_info['port']['bridge_domain_id'],
                                     tap_service_info['port']['iface_idx'])
        self.vppf.vpp.delete_bridge_domain(tap_service_info['service_bridge']['bridge_domain_id'])
        try:
            self.etcd_client.delete(taas_path)
        except etcd.EtcdKeyNotFound:
            # Gone is fine, if we didn't delete it
            # it's no problem
            pass

    def resync(self):
        pass


class TaasFlowAgentFeature(feature.AgentFeature):
    path = 'taas_flow'

    def __init__(self, conf, etcd_client, vppf):
        super(TaasFlowAgentFeature, self).__init__(conf, etcd_client, vppf)
        # is this legal ?
        # /!\ We can NOT create an other instance of VPPInterface
        # as vpp_papi can only be connected once per process !
        self.vppf.vpp.__class__ = VPPInterfaceEx
        self.tap_flows = {}

    def key_set(self, key, value):
        # Create or update == bind
        flow_id = key
        data = json.loads(value)

        taas_id = data['taas_id']
        source_port_uuid = data['tap_flow']['source_port']
        direction = data['tap_flow']['direction']

        port_path = (LEADIN + '/nodes/' + self.host + '/ports/' +
                     str(data['tap_flow']['source_port']))
        port_info = json.loads(self.etcd_client.read(port_path).value)
        physnet = port_info['physnet']

        port_path = (LEADIN + '/state/' + self.host + '/ports/' +
                     str(data['tap_flow']['source_port']))
        port_info = json.loads(self.etcd_client.read(port_path).value)
        source_port_idx = port_info['iface_idx']

        # Get the Tap Service bridge
        service_bridge = self.vppf.network_on_host(physnet, 'vlan',
                                                   taas_id)['bridge_domain_id']

        span_ifidx = self.vppf.vpp.create_loopback()
        self.vppf.vpp.set_interface_tag(span_ifidx, 'flow.' + flow_id)
        self.vppf.vpp.add_to_bridge(service_bridge, span_ifidx)
        self.vppf.vpp.ifup(span_ifidx)
        self.vppf.vpp.enable_port_mirroring(source_port_idx, span_ifidx, direction)

        data['tap_flow']['status'] = 'ACTIVE'
        oper_data = {'port_idx': source_port_idx,
                     'span_idx': span_ifidx,
                     'tap_flow': data['tap_flow']}

        self.etcd_client.write(self._operational_key_space(self.host) +
                                '/%s' % flow_id,
                                json.dumps(oper_data))

    def key_delete(self, key, value):
        # Removing key == desire to unbind
        flow_id = key
        
        taas_path = self._operational_key_space(self.host) + '/' + key
        tap_flow_info = json.loads(self.etcd_client.read(taas_path).value)
        source_port_idx = tap_flow_info['port_idx']
        span_idx = tap_flow_info['span_idx']
        self.vppf.vpp.disable_port_mirroring(source_port_idx, span_idx)
        self.vppf.vpp.delete_loopback(span_idx)

        try:
            self.etcd_client.delete(self._operational_key_space(self.host) +
                                     '/%s' % flow_id)
        except etcd.EtcdKeyNotFound:
            # Gone is fine, if we didn't delete it
            # it's no problem
            pass
