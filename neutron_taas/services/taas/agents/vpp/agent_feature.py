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
import re
from networking_vpp import feature

from oslo_config import cfg
from oslo_log import log as logging

LOG = logging.getLogger(__name__)


tap_services = {}
tap_flows = {}


class VPPInterfaceEx(object):

    def __init__(self, vppinterface):
        self.base_vppi = vppinterface
        self._vpp = vppinterface._vpp

    def __getattr__(self, name):
        # If name begins with f create a method
        if name in self.__dict__:
            return getattr(self, name)
        else:
            return getattr(self.base_vppi, name)

    def create_loopback(self, mac="", tag=""):
        LOG.debug("Creating local interface")
        t = self._vpp.create_loopback(mac_address=mac)
        self._check_retval(t)
        self.set_interface_tag(t.sw_if_index, tag)
        return t.sw_if_index

    def delete_loopback(self, idx):
        LOG.debug("Deleting VPP interface - index: %s" % idx)
        t = self._vpp.delete_loopback(sw_if_index=idx)
        self._check_retval(t)

    def enable_port_mirroring(self, source_idx, dest_idx, direction):
        LOG.debug("Enable span from %d to %d",
                  source_idx, dest_idx)
        if direction == 'IN':
            state = 1
        elif direction == 'OUT':
            state = 2
        else:
            state = 3
        t = self._vpp.sw_interface_span_enable_disable(
            sw_if_index_from=source_idx,
            sw_if_index_to=dest_idx,
            state=state)
        self._check_retval(t)

    def disable_port_mirroring(self, source_idx, dest_idx):
        LOG.debug("Disable span from %d to %d",
                  source_idx, dest_idx)
        t = self._vpp.sw_interface_span_enable_disable(
            sw_if_index_from=source_idx,
            sw_if_index_to=dest_idx,
            state=0)
        self._check_retval(t)

    def dump_port_mirroring(self):
        r = self._vpp.sw_interface_span_dump()
        LOG.info(str(r))

    def bridge_enable_broadcast(self, bridge_domain_id):
        L2_LEARN = (1 << 0)
        L2_FWD = (1 << 1)
        L2_FLOOD = (1 << 2)
        L2_UU_FLOOD = (1 << 3)
        L2_ARP_TERM = (1 << 4)

        LOG.debug("Enable broadcase (disable mac learning) for bridge %d",
                  bridge_domain_id)
        t = self._vpp.bridge_flags(bd_id=bridge_domain_id, is_set=0,
                                   feature_bitmap=(L2_LEARN | L2_FWD |
                                                   L2_FLOOD | L2_UU_FLOOD |
                                                   L2_ARP_TERM))
        t = self._vpp.bridge_flags(bd_id=bridge_domain_id, is_set=1,
                                   feature_bitmap=L2_FLOOD)
        self._check_retval(t)


class VPPForwarderEx(object):

    def __init__(self, vppf):
        self.base_vppf = vppf
        self.vpp = VPPInterfaceEx(vppf.vpp)

    def __getattr__(self, name):
        # If name begins with f create a method
        if name in self.__dict__:
            return getattr(self, name)
        else:
            return getattr(self.base_vppf, name)

    def get_bd_for_iface(self, if_idx):
        for (bd_id, ifaces) in self.vpp.get_ifaces_in_bridge_domains().items():
            if if_idx in ifaces:
                current_bd_id = bd_id
                break
        else:
            current_bd_id = 0
        return current_bd_id

    def create_span_port(self, tag):
        # TODO: setup input/ouput classifier to disable loopback behavior
        span_ifidx = self.vpp.create_loopback(tag=tag)
        self.vpp.ifup(span_ifidx)
        return span_ifidx

    def delete_span_port(self, if_idx):
        return self.vpp.delete_loopback(if_idx)


class TaasServiceAgentFeature(feature.AgentFeature):
    path = 'taas_service'

    taas_opt = [cfg.StrOpt('physnet',
                           help=_("Name of the physnet to use. "
                                  "If not set, we will pick the "
                                  "first defined physnet in "
                                  "'ml2_vpp.physnets'."),
                           default=None),
                cfg.StrOpt('overlay_type',
                           help=_("Defines the overlay type, currently, "
                                  "only 'vlan is supported.'"),
                           default='vlan')]

    def __init__(self, conf, etcd_client, vppf):
        super(TaasServiceAgentFeature, self).__init__(conf,
                                                      etcd_client,
                                                      VPPForwarderEx(vppf))
        conf.register_opts(self.taas_opt, "ml2_taas")
        if conf.ml2_taas.physnet in vppf.physnets.keys():
            self.taas_interface = conf.ml2_taas.physnet
        else:
            # Get first defined interface in physnet list
            LOG.warning('Physnet "%s" not avaialable, '
                        'defaulting to "%s"', conf.ml2_taas.physnet,
                        vppf.physnets.key()[0])
            self.taas_interface = vppf.physnets.key()[0]
        self.network_type = conf.ml2_taas.overlay_type

    def key_set(self, key, value):
        # Create or update == bind
        tap_service_id = key
        data = json.loads(value)
        port_uuid = str(data['tap_service']['port_id'])

        port_sw_if_idx = self.vppf.vpp.get_ifidx_by_tag(port_uuid)
        old_bridge_domain_id = self.vppf.get_bd_for_iface(port_sw_if_idx)

        bridge_data = self.vppf.network_on_host(self.taas_interface,
                                                self.network_type,
                                                data['taas_id'])
        self.vppf.vpp.set_interface_tag(bridge_data['if_upstream_idx'],
                                        '%s.%s' % (self.path, tap_service_id))
        self.vppf.vpp.bridge_enable_broadcast(bridge_data['bridge_domain_id'])
        self.vppf.vpp.add_to_bridge(bridge_data['bridge_domain_id'],
                                    port_sw_if_idx)

        # TODO(cfontaine): check real state before reporting everything is OK
        data['tap_service']['status'] = 'ACTIVE'
        oper_data = {'service_bridge': bridge_data,
                     'port': {'iface_idx': port_sw_if_idx,
                              'bridge_domain_id': old_bridge_domain_id}}

        self._etcd_write(self.host, tap_service_id,
                         json.dumps(oper_data))

    def key_delete(self, key, value):
        # Removing key == desire to unbind
        # rebind iface to appropriate bridge
        taas_path = self._build_path(self.host, key)
        tap_service_info = json.loads(self.etcd_client.read(taas_path).value)
        self.vppf.vpp.add_to_bridge(
            tap_service_info['port']['bridge_domain_id'],
            tap_service_info['port']['iface_idx'])
        self.vppf.vpp.delete_bridge_domain(
            tap_service_info['service_bridge']['bridge_domain_id'])
        try:
            self.etcd_client.delete(taas_path)
        except etcd.EtcdKeyNotFound:
            # Gone is fine, if we didn't delete it
            # it's no problem
            pass

    def resync_start(self):
        tap_services = {}
        m = re.compile('%s\.(.*)' % self.path)
        bridges = self.vppf.vpp.get_ifaces_in_bridge_domains()
        for iface in self.vppf.vpp.get_interfaces():
            g = m.match(iface['tag'])
            if g and iface['sw_if_idx'] in bridges:
                uuid = g.group(1)
                #basename = iface['name'].split('.')[0]
                #vlan = iface['name'].split('.')[1]
                props = {'service_bridge':
                         {'bridge_domain_id': iface['sw_if_idx']},
                         'port': {'iface_idx': iface['sw_if_idx']},
                         'bridge_domain_id': 0xffffffff}
                tap_services[uuid] = props

    def resync_end(self):
        pass


class TaasFlowAgentFeature(feature.AgentFeature):
    path = 'taas_flow'

    def __init__(self, conf, etcd_client, vppf):
        super(TaasFlowAgentFeature, self).__init__(conf,
                                                   etcd_client,
                                                   VPPForwarderEx(vppf))

    def key_set(self, key, value):
        # Create or update == bind
        flow_id = key
        data = json.loads(value)

        taas_id = data['taas_id']
        source_port_uuid = data['tap_flow']['source_port']
        direction = data['tap_flow']['direction']

        source_port_idx = self.vppf.get_ifidx_by_tag(source_port_uuid)
        # Get the Tap Service bridge
        bd_id = tap_services[taas_id]['service_bridge']['bridge_domain_id']

        span_ifidx = self.vppf.create_span_port('%s.%s' %
                                                (self.path, flow_id))
        self.vppf.vpp.add_to_bridge(bd_id, span_ifidx)

        self.vppf.vpp.enable_port_mirroring(source_port_idx,
                                            span_ifidx,
                                            direction)

        data['tap_flow']['status'] = 'ACTIVE'
        oper_data = {'port_idx': source_port_idx,
                     'span_idx': span_ifidx,
                     'tap_flow': data['tap_flow']}

        tap_flows[flow_id] = oper_data

        self.etcd_client.write(self.host, flow_id,
                               json.dumps(oper_data))

    def key_delete(self, key, value):
        # Removing key == desire to unbind
        flow_id = key
        tap_flows[flow_id]

        self.vppf.vpp.disable_port_mirroring(tap_flows[flow_id]['port_idx'],
                                             tap_flows[flow_id]['span_idx'])
        self.vppf.vpp.delete_loopback(tap_flows[flow_id]['span_idx'])

        try:
            self.etcd_client.delete(self._build_path(self.host, key))
        except etcd.EtcdKeyNotFound:
            # Gone is fine, if we didn't delete it
            # it's no problem
            pass

    def resync_start(self):
        global tap_flows
        tap_flows = {}
        pass

    def resync_end(self):
        pass
