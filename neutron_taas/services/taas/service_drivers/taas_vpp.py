# Copyright (C) 2016 Qosmos
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""VPP Taas service plugin."""
import eventlet
import threading
import json
from networking_vpp import feature
from networking_vpp import mech_vpp
from neutron import context
from neutron_lib import constants
from neutron_lib import exceptions as n_exc
from neutron_taas.extensions import taas as taas_ex
from neutron_taas.services.taas import service_drivers

from oslo_config import cfg
from oslo_log import log as logging

LOG = logging.getLogger(__name__)


COMMUNICATION_TIMEOUT = 60

class FeatureTaasService(feature.ServerFeature):
    """FeatureTaasService.
    
    Server side of the TaaS Service functionnality.
    """

    path = 'taas_service'

    def key_set(self, host, key, value):
        """Called when the service has been created."""
        LOG.info('FeatureTaasService set %s %s', key, str(value))
        if value is None:
            return
        data = json.loads(value)
        data['tap_service']['status'] = constants.ACTIVE
        self.service_plugin.update_tap_service(self._context, key, data)

    def key_delete(self, host, key, value):
        """Called when the service has been deleted."""
        LOG.info('FeatureTaasService delete %s' % str(key))

    def create(self, port, taas_data):
        """Server to compute node - creation request."""
        host = port['binding:host_id']
        service_id = taas_data['tap_service']['id']
        self._etcd_write(host, service_id, taas_data)

    def delete(self, host, taas_data):
        """Server to compute node - deletion request."""
        service_id = taas_data['tap_service']['id']
        self._etcd_write(host, service_id, None)


class FeatureTaasFlow(feature.ServerFeature):
    """FeatureTaasFlow.
    
    Server side of the TaaS Flow functionnality.
    """

    path = 'taas_flow'

    def key_set(self, host, key, value):
        """Called when the flow has been created."""
        LOG.info('FeatureTaasFlow set %s %s', str(key), str(value))
        if value is None:
            return
        data = json.loads(value)
        data['tap_flow']['status'] = constants.ACTIVE
        self.service_plugin.update_tap_flow(self._context, key, data)

    def key_delete(self, host, key, value):
        """Called when the flow has been deleted."""
        LOG.info('FeatureTaasFlow delete %s' % str(key))

    def create(self, port, data):
        """Server to compute node - creation request."""
        host = port['binding:host_id']
        flow_id = data['tap_flow']['id']
        self._etcd_write(host, flow_id, data)

    def delete(self, host, flow_id):
        """Server to compute node - deletion request."""
        self._etcd_write(host, flow_id, None)


class TaasEtcdDriver(service_drivers.TaasBaseDriver):
    """Taas Etcd Service Driver class."""

    def __init__(self, service_plugin):
        """"Init method."""
        LOG.debug("Loading TaasEtcdDriver.")
        super(TaasEtcdDriver, self).__init__(service_plugin)
        self.communicator = mech_vpp.EtcdAgentCommunicator()

        self.taas_service = FeatureTaasService(cfg.CONF, self.communicator)
        self.taas_flow = FeatureTaasFlow(cfg.CONF, self.communicator)

        self.taas_service.service_plugin = self.service_plugin
        self.taas_flow.service_plugin = self.service_plugin

        self.communicator.register_feature(self.taas_service)
        self.communicator.register_feature(self.taas_flow)

    def _get_taas_id(self, context, tf):
        ts = self.service_plugin.get_tap_service(context,
                                                 tf['tap_service_id'])
        taas_id = (self.service_plugin.get_tap_id_association(
            context,
            tap_service_id=ts['id'])['taas_id'] +
            cfg.CONF.taas.vlan_range_start)
        return taas_id

    def create_tap_service_precommit(self, context):
        """Send tap service creation RPC message to agent.

        This RPC message includes taas_id that is added vlan_range_start to
        so that taas-ovs-agent can use taas_id as VLANID.
        """
        # by default, the status is ACTIVE: wait for creation...
        context.tap_service['status'] = constants.PENDING_CREATE
        ts = context.tap_service
        # Get taas id associated with the Tap Service
        tap_id_association = context._plugin.create_tap_id_association(
            context._plugin_context, ts['id'])
        context.tap_id_association = tap_id_association
        self.service_plugin.update_tap_service(context._plugin_context, ts['id'],
                                               {'tap_service': context.tap_service})
        ts = context.tap_service
        tap_id_association = context.tap_id_association
        taas_vlan_id = (tap_id_association['taas_id'] +
                        cfg.CONF.taas.vlan_range_start)
        port = self.service_plugin._get_port_details(context._plugin_context,
                                                     ts['port_id'])

        if taas_vlan_id > cfg.CONF.taas.vlan_range_end:
            raise taas_ex.TapServiceLimitReached()

        rpc_msg = {'tap_service': ts,
                   'taas_id': taas_vlan_id,
                   'port': port}

        # TODO(cfontaine): is there a better way to get 
        # the context in the callback ?
        self.taas_service.create(port, rpc_msg)
        return

    def create_tap_service_postcommit(self, context):
        """Send tap service creation RPC message to agent."""
        pass

    def delete_tap_service_precommit(self, context):
        """Send tap service deletion RPC message to agent.

        This RPC message includes taas_id that is added vlan_range_start to
        so that taas-ovs-agent can use taas_id as VLANID.
        """
        ts = context.tap_service
        tap_id_association = context.tap_id_association
        taas_vlan_id = (tap_id_association['taas_id'] +
                        cfg.CONF.taas.vlan_range_start)
        try:
            port = self.service_plugin._get_port_details(
                context._plugin_context,
                ts['port_id'])
            host = port['binding:host_id']
        except n_exc.PortNotFound:
            # if not found, we just pass to None
            port = None
            host = None

        rpc_msg = {'tap_service': ts,
                   'taas_id': taas_vlan_id,
                   'port': port}

        self.taas_service.delete(host, rpc_msg)
        return

    def delete_tap_service_postcommit(self, context):
        """Send tap service deletion RPC message to agent."""
        pass

    def create_tap_flow_precommit(self, context):
        """Send tap flow creation RPC message to agent."""
        tf = context.tap_flow
        tf['status'] = constants.PENDING_CREATE
        taas_id = self._get_taas_id(context._plugin_context, tf)
        
        self.service_plugin.update_tap_flow(context._plugin_context, tf['id'], {'tap_flow': tf})
        # Extract the host where the source port is located
        port = self.service_plugin._get_port_details(context._plugin_context,
                                                     tf['source_port'])
        # host = port['binding:host_id']
        port_mac = port['mac_address']

        # This status will be set in the callback
        rpc_msg = {'tap_flow': tf,
                   'port_mac': port_mac,
                   'taas_id': taas_id,
                   'port': port}
        self.taas_flow.create(port, rpc_msg)
        return

    def create_tap_flow_postcommit(self, context):
        """Send tap flow creation RPC message to agent."""
        pass

    def delete_tap_flow_precommit(self, context):
        """Send tap flow deletion RPC message to agent."""
        tf = context.tap_flow
        taas_id = self._get_taas_id(context._plugin_context, tf)
        # Extract the host where the source port is located
        port = self.service_plugin._get_port_details(context._plugin_context,
                                                     tf['source_port'])
        host = port['binding:host_id']
        port_mac = port['mac_address']
        # Send RPC message to both the source port host and
        # tap service(destination) port host
        rpc_msg = {'tap_flow': tf,
                   'port_mac': port_mac,
                   'taas_id': taas_id,
                   'port': port}

        self.taas_flow.delete(host, tf['id'])
        return

    def delete_tap_flow_postcommit(self, context):
        """Send tap flow deletion RPC message to agent."""
        pass
