__author__ = 'devopsjedi'

import sys
import yaml
import logging
from schema import Schema, And, Use, Or, Optional, SchemaError
import socket
from time import strftime
import yaml
import yaml.constructor

try:
    # included in standard lib from Python 2.7
    from collections import OrderedDict
except ImportError:
    # try importing the backported drop-in replacement
    # it's available on PyPI
    from ordereddict import OrderedDict


class OrderedDictYAMLLoader(yaml.Loader):
    """
    A YAML loader that loads mappings into ordered dictionaries.
    """

    def __init__(self, *args, **kwargs):
        yaml.Loader.__init__(self, *args, **kwargs)

        self.add_constructor(u'tag:yaml.org,2002:map', type(self).construct_yaml_map)
        self.add_constructor(u'tag:yaml.org,2002:omap', type(self).construct_yaml_map)

    def construct_yaml_map(self, node):
        data = OrderedDict()
        yield data
        value = self.construct_mapping(node)
        data.update(value)

    def construct_mapping(self, node, deep=False):
        if isinstance(node, yaml.MappingNode):
            self.flatten_mapping(node)
        else:
            raise yaml.constructor.ConstructorError(None, None,
                'expected a mapping node, but found %s' % node.id, node.start_mark)

        mapping = OrderedDict()
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                hash(key)
            except TypeError, exc:
                raise yaml.constructor.ConstructorError('while constructing a mapping',
                    node.start_mark, 'found unacceptable key (%s)' % exc, key_node.start_mark)
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping

LOG_FILENAME = 'applyNetscalerState_{}.log'.format(strftime("%Y%m%d_%H%M%S"))

from nsnitro.nsnitro import NSNitro
from nsnitro.nsexceptions import NSNitroError
from nsnitro.nsresources.nsserver import NSServer
from nsnitro.nsresources.nsservicegroup import NSServiceGroup
from nsnitro.nsresources.nsservicegroupserverbinding import NSServiceGroupServerBinding
from nsnitro.nsresources.nslbvserver import NSLBVServer
from nsnitro.nsresources.nslbvserverservicegroupbinding import NSLBVServerServiceGroupBinding
from nsnitro.nsresources.nscspolicy import NSCSPolicy
from nsnitro.nsresources.nscsvserver import NSCSVServer
from nsnitro.nsresources.nscsvservercspolicybinding import NSCSVServerCSPolicyBinding
from nsnitro.nsresources.nsbaseresource import NSBaseResource
#from nitro.nsresources.nssslvserversslcertkeybinding import NSSSLVServerSSLCertKeyBinding

from nssrc.com.citrix.netscaler.nitro.service.nitro_service import nitro_service
from nssrc.com.citrix.netscaler.nitro.service.nitro_service import nitro_exception
from nssrc.com.citrix.netscaler.nitro.resource.config.basic.server import server

_mapping_tag = yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG
'''
def dict_representer(dumper, data):
    return dumper.represent_dict(data.iteritems())

def dict_constructor(loader, node):
    return collections.OrderedDict(loader.construct_pairs(node))

yaml.add_representer(collections.OrderedDict, dict_representer)
yaml.add_constructor(_mapping_tag, dict_constructor)
'''

log = logging.getLogger('applyNetscalerState')
log.setLevel(logging.DEBUG)
stream = logging.StreamHandler()
stream.setLevel(logging.DEBUG)
file = logging.FileHandler(LOG_FILENAME)
log.addHandler(stream)
log.addHandler(file)


def get_config_yaml(filename):
    '''

    :param filename: filename of YAML state declaration file
    :return: object containing config items
    '''

    ret = None
    try:
        stream = open(filename,"r")
        try:
            conf = ordered_load(stream,Loader=yaml.SafeLoader)
            ret = conf
        except yaml.YAMLError as error:
            log.info('YAML import failed: {}'.format(error.message))
            conf = None
        stream.close()
    except IOError as error:
        log.info('Failed to open {}'.format(filename))
    return conf


def update_yaml(conf,filename):
    ret = None
    try:
        stream = open(filename,"w")
        try:
            ordered_dump(conf,stream,Dumper=yaml.SafeDumper)
            ret = True
        except yaml.YAMLError as error:
            log.info('YAML export failed: {}'.format(error.message))
        stream.close()
    except IOError as error:
        log.info('Failed to open {}'.format(filename))

    return conf


def ordered_load(stream, Loader=yaml.Loader, object_pairs_hook=OrderedDict):
    class OrderedLoader(Loader):
        pass
    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return object_pairs_hook(loader.construct_pairs(node))
    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_mapping)
    return yaml.load(stream, OrderedLoader)

def ordered_dump(data, stream=None, Dumper=yaml.Dumper, **kwds):
    class OrderedDumper(Dumper):
        pass
    def _dict_representer(dumper, data):
        return dumper.represent_mapping(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, data.items())
    OrderedDumper.add_representer(OrderedDict, _dict_representer)
    return yaml.dump(data, stream, OrderedDumper, default_flow_style=False)


def connect(ns_instance):
    '''

    :param ns_instance: NSNitro instance
    :return: Connected NSNitro instance
    '''
    nitro = NSNitro(ns_instance['address'],ns_instance['user'],ns_instance['pass'])
    try:
        nitro.login()
    except NSNitroError as error:
        log.debug('NSNitro.login() failed: {0}'.format(error))
    return nitro


def disconnect(nitro):
    '''

    :param nitro: NSNitro instance
    :return:  Disconnected NSNitro instance
    '''
    try:
        nitro.logout()
    except NSNitroError as error:
        log.debug('NSNitro.logout() failed: {0}'.format(error))
        return None
    return nitro


def ensure_server_state(nitro, server_conf):
    '''

    :param nitro: NSNitro instance
    :param server_conf: Server configuration item from input file
    :return: True if configuration is applied successfully; False otherwise

    Validates NS configuration and applies any changes needed to match the input server configuration item.
    - If a server name and IP match the input configuration, nothing changes.
    - If a server name matches the input configuration but the IP doesn't, the IP is updated.
    - If an existing server has an IP address used in the configuration but the name doesn't match, the existing server is deleted and a new server is created. 
    '''
    ret = True
    
    all_servers = NSServer.get_all(nitro)
    matches_found = {}
    for server in all_servers:
      if server.get_name() == server_conf['name'] and server.get_ipaddress() == server_conf['ip_address']:
          matches_found['full_match'] = server
      elif server.get_name() == server_conf['name']:
          matches_found['name'] = server
      elif server.get_ipaddress() == server_conf['ip_address']:
          matches_found['ip_address'] = server

    if matches_found.has_key('full_match'):
        return True
    else:
        update = False
        if matches_found.has_key('ip_address') and not matches_found.has_key('name'):
            server_to_delete = matches_found['ip_address']
            try:
                NSServer.delete(nitro,server_to_delete)
            except NSNitroError as error:
                log.debug('NSServer.delete() failed: {0}'.format(error))
                ret = False
        if matches_found.has_key('name') and not matches_found.has_key('ip_address'):
            update = True
            updated_server = matches_found['name']
            updated_server.set_ipaddress(server_conf['ip_address'])
            updated_server.set_translationip(None)
            updated_server.set_translationmask(None)
            try:
                NSServer.update(nitro, updated_server)
            except NSNitroError as error:
                log.debug('NSServer.update() failed: {0}'.format(error))
                ret = False
        if matches_found.has_key('name') and matches_found.has_key('ip_address'):
            update = True
            server_to_delete = matches_found['ip_address']
            try:
                NSServer.delete(nitro,server_to_delete)
            except NSNitroError as error:
                log.debug('NSServer.delete() failed: {0}'.format(error))
                ret = False 
            updated_server = matches_found['name']
            updated_server.set_ipaddress(server_conf['ip_address'])
            updated_server.set_translationip(None)
            updated_server.set_translationmask(None)
            try:
                NSServer.update(nitro, updated_server)
            except NSNitroError as error:
                log.debug('NSServer.update() failed: {0}'.format(error))
                ret = False
        if not update:
            new_server = NSServer()
            new_server.set_name(server_conf['name'])
            new_server.set_ipaddress(server_conf['ip_address'])
            try:
                NSServer.add(nitro,new_server)
            except NSNitroError as error:
                log.debug('NSServer.add() failed: {0}'.format(error))
                ret = False

    
    return ret

def create_ns_resource_object(ns_resource_object,object_conf):
    options = map_yaml_config_to_nitro_object_options(object_conf)
    ns_resource_object.set_options(object_conf)

def map_yaml_config_to_nitro_object_options(resource_type,object_conf):
    options = {}
    for property in rw_properties[resource_type]:
        nitro_property = property['nitro']
        yaml_property = property['yaml']
        options[nitro_property] = object_conf[yaml_property]
    return options


def map_nitro_object_options_to_yaml_config(resource_type,nitro_object):
    yaml_config = {}
    for property in rw_properties[resource_type]:
        nitro_property = property['nitro']
        yaml_property = property['yaml']
        yaml_config[yaml_property] = nitro_object.options[nitro_property]
    return yaml_config


def ensure_service_group_state(nitro, service_group_conf):
    '''

    :param nitro: NSNitro instance
    :param service_group_conf: Service Group configuration item from input file
    :return: True if configuration is applied successfully; False otherwise

    Validates NS configuration and applies any changes needed to match the input service group configuration item.
    - Creates a new service group configuration if a matching name is not found in existing service groups.
    - If an existing service group is found with same name as configuration item, the service type is validated to match the protocol in the configuration item
    - A new service group is created if no matching service group exists
    - Service group binding to servers is validated and updated if necessary

    '''
    ret = True
    existing_service_group = NSServiceGroup()

    existing_service_group.set_servicegroupname(service_group_conf['name'])
    try:
        existing_service_group = NSServiceGroup.get(nitro,existing_service_group)
    except NSNitroError as error:
        log.debug('no existing service_group found for {}'.format(service_group_conf['name']))
        existing_service_group = None

    if existing_service_group:
        if existing_service_group.get_servicetype() != service_group_conf['protocol']:
            try:
                NSServiceGroup.delete(nitro, existing_service_group)
            except NSNitroError as error:
                log.debug('NSServiceGroup.delete() failed: {0}'.format(error))
                ret = False
            existing_service_group = None

    if not existing_service_group:
        new_service_group = NSServiceGroup()
        new_service_group.set_servicegroupname(service_group_conf['name'])
        new_service_group.set_servicetype(service_group_conf['protocol'])
        try:
            NSServiceGroup.add(nitro, new_service_group)
        except NSNitroError as error:
            log.debug('NSServiceGroup.add() failed: {0}'.format(error))
            ret = False
        current_service_group= new_service_group

    existing_bindings = get_bindings_for_service_group(nitro,service_group_conf)

    if existing_bindings != None:
        bindings_to_remove = []
        for existing_binding in existing_bindings:
            binding_found = False
            for server_binding_conf in service_group_conf['servers']:
                if existing_binding == server_binding_conf:
                    binding_found = True
                    server_binding_conf['bound'] = True
                elif existing_binding['name'] == server_binding_conf['name']:
                    updated_binding = create_ns_resource_object(NSServiceGroupServerBinding(),existing_binding)
                    try:
                        NSServiceGroupServerBinding.update(nitro, binding)
                        server_binding_conf['bound'] = True
                    except NSNitroError as error:
                        log.debug('NSServiceGroupServerBinding.update() failed: {0}'.format(error))
                        ret = False
            if not binding_found:
                binding_to_remove = binding
                try:
                    NSServiceGroupServerBinding.delete(nitro, binding_to_remove)
                except NSNitroError as error:
                    log.debug('NSServiceGroupServerBinding.remove() failed: {0}'.format(error))
                    ret = False


        for server_binding_conf in service_group_conf['servers']:
            if not server_binding_conf.has_key('bound'):
                new_binding = NSServiceGroupServerBinding()
                new_binding.set_servicegroupname(service_group_conf['name'])
                new_binding.set_servername(server_binding_conf['name'])
                new_binding.set_port(server_binding_conf['port'])
                try:
                    NSServiceGroupServerBinding.add(nitro, new_binding)
                except NSNitroError as error:
                    log.debug('NSServiceGroupServerBinding.add() failed: {0}'.format(error))
                    ret = False
            else:
                server_binding_conf.pop('bound')

    
    return ret

def ensure_service_groups_state(nitro,service_groups_conf):
    '''

    :param nitro: NSNitro instance
    :param sservice_groups_conf: List of server configuration items from input file
    :return: True if configuration is applied successfully; False otherwise

    Validates NS configuration and applies any changes needed to match the list of input service group configuration items.
    - Checks for matching service group names between NS and config file
    - Each server group config item in list is sent to ensure_service_group_state()
    - Existing service groups that do not match the config are deleted
    '''
    ret = True
    all_service_groups = NSServiceGroup.get_all(nitro)
    service_groups_to_remove = []
    for service_group in all_service_groups:
        found_match = False
        for service_group_conf in service_groups_conf:
            if service_group_conf['name'] == service_group.get_servicegroupname():
                found_match = True
        if not found_match:
            service_groups_to_remove.append(service_group)

    for service_group_conf in service_groups_conf:
        ensure_service_group_state(nitro,service_group_conf)

    for service_group_to_remove in service_groups_to_remove:
        try:
            NSServiceGroup.delete(nitro, service_group_to_remove)
        except NSNitroError as error:
            log.debug('NSServiceGroupServerBinding.delete() failed: {0}'.format(error))
            ret = False

    
    return ret

def ensure_servers_state(nitro,servers_conf):
    '''

    :param nitro: NSNitro instance
    :param servers_conf: List of server configuration items from input file
    :return: True if configuration is applied successfully; False otherwise

    Validates NS configuration and applies any changes needed to match the list of input server configuration items.
    - Sends all server config items to ensure_server_state()
    - Iterates through list of server config items to find matching names in existing servers on the NS
    - Deletes existing servers on the NS that do not match any server config items
    '''
    ret = True
    for server_conf in servers_conf:
        ensure_server_state(nitro,server_conf)

    all_servers = NSServer.get_all(nitro)
    servers_to_remove = []
    for server in all_servers:
        found_match = False
        for server_conf in servers_conf:
            if server_conf['name'] == server.get_name():
                found_match = True
        if not found_match:
            servers_to_remove.append(server)

    for server_to_remove in servers_to_remove:
        try:
            NSServer.delete(nitro, server_to_remove)
        except NSNitroError as error:
            log.debug('NSServer.delete() failed: {0}'.format(error))
            ret = False

    
    return ret

def ensure_lbvservers_state(nitro,lbvservers_conf):
    '''

    :param nitro: NSNitro instance
    :param lbvservers_conf: List of load balancing virtual server configuration items from input file
    :return: True if configuration is applied successfully; False otherwise

    Validates NS configuration and applies any changes needed to match the list of input lbvserver configuration items.
    - Sends all lbvserver config items to ensure_lbvserver_state()
    - Iterates through list of lbvserver config items to find matching names in existing lbvservers on the NS
    - Deletes existing lbvservers on the NS that do not match any lbvserver config items
    '''
    ret = True
    for lbvserver_conf in lbvservers_conf:
        ensure_lbvserver_state(nitro,lbvserver_conf)

    all_lbvservers = NSLBVServer.get_all(nitro)
    lbvservers_to_remove = []
    for lbvserver in all_lbvservers:
        found_match = False
        for lbvserver_conf in lbvservers_conf:
            if lbvserver_conf['name'] == lbvserver.get_name():
                found_match = True
        if not found_match:
            lbvservers_to_remove.append(lbvserver)

    for lbvserver_to_remove in lbvservers_to_remove:
        try:
            NSLBVServer.delete(nitro, lbvserver_to_remove)
        except NSNitroError as error:
            log.debug('NSLBVServer.delete() failed: {0}'.format(error))
            ret = False

    
    return ret

def ensure_lbvserver_state(nitro, lbvserver_conf):
    '''

    :param nitro: NSNitro instance
    :param lbvserver_conf: Load balancing virtual server configuration item from input file
    :return: True if configuration is applied successfully; False otherwise

    Validates NS configuration and applies any changes needed to match the input lbvserver configuration item.
    - Iterates through existing lbvservers on the NS to find matching name and/or IP address contained in the lbvserver config item
    - An existing lbvserver on the NS with a matching name is validated and updated if necessary to match the IP of the lbvserver config item
    - An existing lbvserver on the NS is deleted if it:
        - Does not have a name that matches the lbvserver config item
        - Does have an IP address the matches the lbvserver config item
    - Creates an lbvserver on the NS using the lbvserver config item if one does not exist
    - Iterates through lbvserver bindings to service groups in the config item
    - Deletes existing lbvserver bindings on the NS that do not match the lbvserver config item
    - Creates lbvserver binding on the NS for each lbvserver config item binding that does not exist
    '''
    ret = True
    
    
    all_lbvservers = NSLBVServer.get_all(nitro)
    matches_found = {}
    for lbvserver in all_lbvservers:
      if lbvserver.get_name() == lbvserver_conf['name'] and lbvserver.get_ipv46() == lbvserver_conf['vip_address'] and lbvserver.get_port == lbvserver_conf['port'] and lbvserver.get_servicetype == lbvserver_conf['protocol']:
          matches_found['full_match'] = lbvserver
      elif lbvserver.get_name() == lbvserver_conf['name']:
          matches_found['name'] = lbvserver
      elif lbvserver.get_ipv46() == lbvserver_conf['vip_address']:
          matches_found['vip_address'] = lbvserver

    if not matches_found.has_key('full_match'):

        check_for_port_and_protocol_match = False

        if matches_found.has_key('vip_address') and not matches_found.has_key('name'):
            updated_lbvserver = matches_found['vip_address']
            updated_lbvserver.set_newname(lbvserver_conf['name'])
            try:
                NSLBVServer.rename(nitro, updated_lbvserver)
            except NSNitroError as error:
                log.debug('NSLBVServer.update() failed: {0}'.format(error))
                ret = False
            lbvserver = NSLBVServer()
            lbvserver.set_name(lbvserver_conf['name'])
            updated_lbvserver = NSLBVServer.get(nitro,lbvserver)
            check_for_port_and_protocol_match = True

        if matches_found.has_key('name') and not matches_found.has_key('vip_address'):
            updated_lbvserver = matches_found['name']
            updated_lbvserver.set_ipv46(lbvserver_conf['vip_address'])
            try:
                NSLBVServer.update(nitro, updated_lbvserver)
            except NSNitroError as error:
                log.debug('NSLBVServer.update() failed: {0}'.format(error))
                ret = False
            check_for_port_and_protocol_match = True

        if matches_found.has_key('name') and matches_found.has_key('vip_address'):
            update = True
            lbvserver_to_delete = matches_found['vip_address']
            try:
                NSLBVServer.delete(nitro,lbvserver_to_delete)
            except NSNitroError as error:
                log.debug('NSLBVServer.delete() failed: {0}'.format(error))
                ret = False 
            updated_lbvserver = matches_found['name']
            updated_lbvserver.set_ipv46(lbvserver_conf['vip_address'])
            try:
                NSLBVServer.update(nitro, updated_lbvserver)
            except NSNitroError as error:
                log.debug('NSLBVServer.update() failed: {0}'.format(error))
                ret = False
            check_for_port_and_protocol_match = True
        if check_for_port_and_protocol_match:
            update = False
            if updated_lbvserver.get_port() != lbvserver_conf['port']:
                update = True
                updated_lbvserver.set_port(lbvserver_conf['port'])
            if updated_lbvserver.get_servicetype() != lbvserver_conf['protocol']:
                update = True
                updated_lbvserver.set_servicetype(lbvserver_conf['protocol'])
            if update:
                try:
                    NSLBVServer.update(nitro, updated_lbvserver)
                except NSNitroError as error:
                    log.debug('NSLBVServer.update() failed: {0}'.format(error))
                    ret = False

    existing_lbvserver = NSLBVServer()
    existing_lbvserver.set_name(lbvserver_conf['name'])

    try:
        existing_lbvserver = NSLBVServer.get(nitro,existing_lbvserver)
    except NSNitroError as error:
        log.debug('no existing lbvserver found for {}'.format(lbvserver_conf['name']))
        existing_lbvserver = None

    if existing_lbvserver:
        update_needed = False
        delete_needed = False
        if existing_lbvserver.get_servicetype() != lbvserver_conf['protocol'] or existing_lbvserver.get_port() != lbvserver_conf['port']:
            delete_needed = True
        elif existing_lbvserver.get_ipv46() != lbvserver_conf['vip_address']:
            update_needed = True
            existing_lbvserver.set_ipv46(lbvserver_conf['vip_address'])
        if delete_needed:
            try:
                NSLBVServer.delete(nitro, existing_lbvserver)
            except NSNitroError as error:
                log.debug('NSLBVServer.delete() failed: {0}'.format(error))
                ret = False
            existing_lbvserver = None
        elif update_needed:
            try:
                NSLBVServer.update(nitro, existing_lbvserver)
            except NSNitroError as error:
                log.debug('NSLBVServer.update() failed: {0}'.format(error))
                ret = False

    if not existing_lbvserver:
        new_lbvserver = NSLBVServer()
        new_lbvserver.set_name(lbvserver_conf['name'])
        new_lbvserver.set_ipv46(lbvserver_conf['vip_address'])
        new_lbvserver.set_port(lbvserver_conf['port'])
        new_lbvserver.set_servicetype(lbvserver_conf['protocol'])
        try:
            NSLBVServer.add(nitro, new_lbvserver)
        except NSNitroError as error:
            log.debug('NSLBVServer.add() failed: {0}'.format(error))
            ret = False
        current_lbvserver= new_lbvserver

    lbvserver_service_group_binding = NSLBVServerServiceGroupBinding()
    lbvserver_service_group_binding.set_name(lbvserver_conf['name'])
    try:
        bindings = NSLBVServerServiceGroupBinding.get(nitro, lbvserver_service_group_binding)
    except NSNitroError as error:
        log.debug('no existing lbvserver server bindings found for {}'.format(lbvserver_conf['name']))
        bindings = None

    lbvserver_service_group_bindings = {}
    for lbvserver_service_group_binding in lbvserver_conf['service_group_bindings']:
        lbvserver_service_group_bindings[lbvserver_service_group_binding] = None

    if bindings != None:
        bindings_to_remove = []
        for binding in bindings:
            binding_found = False
            for lbvserver_service_group_binding in lbvserver_service_group_bindings.keys():
                if binding.get_servicegroupname() == lbvserver_service_group_binding:
                    binding_found = True
                    lbvserver_service_group_bindings[lbvserver_service_group_binding] = True
            if not binding_found:
                binding_to_remove = binding
                try:
                    NSLBVServerServiceGroupBinding.delete(nitro, binding_to_remove)
                except NSNitroError as error:
                    log.debug('NSLBVServerServiceGroupBinding.remove() failed: {0}'.format(error))
                    ret = False

    for lbvserver_service_group_binding in lbvserver_service_group_bindings.keys():
        if lbvserver_service_group_bindings[lbvserver_service_group_binding] == None:
            new_binding = NSLBVServerServiceGroupBinding()
            new_binding.set_name(lbvserver_conf['name'])
            new_binding.set_servicegroupname(lbvserver_service_group_binding)
            try:
                NSLBVServerServiceGroupBinding.add(nitro, new_binding)
            except NSNitroError as error:
                log.debug('NSLBVServerServiceGroupBinding.add() failed: {0}'.format(error))
                ret = False

    
    return ret


def add_cs_action(nitro,cs_action_conf):
    '''

    :param nitro: NSNitro instance
    :param cs_action_conf: Content Switching Action configuration item from input file
    :return: True if configuration is applied successfully; False otherwise

    Creates new Content Switching Action from cs_action configuration item
    - No NSNitro object implements this functionality
    - Used to implement CRUD capability for CS Actions
    '''
    ret = True
    cs_action = NSBaseResource()
    cs_action.set_options({'name':cs_action_conf['name'], 'targetlbvserver':cs_action_conf['target_lbvserver']})
    cs_action.resourcetype = 'csaction'
    try:
        cs_action.add_resource(nitro)
    except NSNitroError as error:
        log.debug('NSBaseResource.add_resource() failed: {0}'.format(error))
        ret = False
    return ret
    

def get_cs_action(nitro,cs_action_name):
    '''

    :param nitro: NSNitro instance
    :param cs_action_name: Content Switching Action name
    :return: NSBaseResource containing CS Action attributes obtained from NS

    Gets Content Switching Action from NS using name
    - No NSNitro object implements this functionality
    - Used to implement CRUD capability for CS Actions
    '''
    ret = None
    cs_action = NSBaseResource()
    cs_action.resourcetype = 'csaction'
    try:
        cs_action.get_resource(nitro, cs_action_name)
        ret = cs_action
    except NSNitroError as error:
        pass

    return ret

def delete_cs_action(nitro,cs_action_name):
    '''

    :param nitro: NSNitro instance
    :param cs_action_name: Content Switching Action name
    :return: True if delete is successful; False otherwise

    Deletes Content Switching Action from NS using name
    - No NSNitro object implements this functionality
    - Used to implement CRUD capability for CS Actions
    '''
    ret = True
    cs_action = NSBaseResource()
    cs_action.resourcetype = 'csaction'
    cs_action.options = {'name':cs_action_name}
    try:
        cs_action.delete_resource(nitro)
    except NSNitroError as error:
        log.debug('NSBaseResource.delete_resource() failed: {0}'.format(error))
        ret = False
    return ret

def update_cs_action(nitro,cs_action):
    '''

    :param nitro: NSNitro instance
    :param cs_action: Content Switching Action object (NSBaseResource)
    :return: True if updated successfully; False otherwise

    Updates existing Content Switching Action on NS
    - No NSNitro object implements this functionality
    - Used to implement CRUD capability for CS Actions
    '''
    ret = True
    updated_cs_action = NSBaseResource()
    updated_cs_action.resourcetype = 'csaction'
    updated_cs_action.options = {'name':cs_action.options['name'],'targetlbvserver':cs_action.options['targetlbvserver']}
    try:
        updated_cs_action.update_resource(nitro)
    except NSNitroError as error:
        log.debug('NSBaseResource.update_resource() failed: {0}'.format(error))
        ret = False
    return ret

def ensure_cs_action_state(nitro,cs_action_conf):
    '''

    :param nitro: NSNitro instance
    :param cs_action_conf: Content Switching Action config item from input file
    :return: True if configuration is applied successfully; False otherwise

    Validates NS configuration and applies any changes needed to match the input cs_action configuration item.
   - Checks for an existing CS Action on the NS and validates/updates the targetlbvserver parameter
   - Creates new CS Action on NS for cs_action config item without an existing match
   '''
    ret = True
    existing_cs_action = get_cs_action(nitro,cs_action_conf['name'])

    if existing_cs_action:
        if existing_cs_action.options['targetlbvserver'] != cs_action_conf['target_lbvserver']:
            existing_cs_action.options['targetlbvserver'] = cs_action_conf['target_lbvserver']
            update_cs_action(nitro, existing_cs_action)
    else:
        ret = add_cs_action(nitro, cs_action_conf)
    return ret



def get_all_cs_actions(nitro):
    '''
    :param nitro: NSNitro object
    :return: list of csaction objects
    Creates custom url string to get all csaction objects from the Netscaler.  Copies name and targetlbvserver options
    to empty csaction objects to avoid update issues with read-only options present.
    '''
    all_cs_actions = []
    resource_type = 'csaction'
    url = nitro.get_url() + resource_type
    cs_actions_options = nitro.get(url).get_response_field(resource_type)

    if cs_actions_options:
        for cs_action_options in cs_actions_options:
            cs_action = NSBaseResource()
            cs_action.resourcetype = resource_type
            cs_action.options = {'name':cs_action_options['name'],'targetlbvserver':cs_action_options['targetlbvserver']}
            all_cs_actions.append(cs_action)
    return all_cs_actions


def ensure_cs_actions_state(nitro, cs_actions_conf):
    '''

    :param nitro: NSNitro instance
    :param cs_actions_conf: List of Content Switching Action configuration items from input file
    :return: True if configuration is applied successfully; False otherwise

    Validates NS configuration and applies any changes needed to match the input list of CS Action configuration items.
   - Iterates through list of cs_action config items and matches them to existing CS Actions on NS
   - Sends cs_action config items that match existing CS Actions on NS to ensure_cs_action_state()
   - Deletes existing CS Actions from NS that do not match cs_action config items in list
   - Sends cs_action config items that do not exist on the NS to ensure_cs_action_state()
   '''
    ret = True

    all_cs_actions = get_all_cs_actions(nitro)
    cs_actions_to_delete = []

    if all_cs_actions != None:
        for existing_cs_action in all_cs_actions:
            found_match = False
            for cs_action_conf in cs_actions_conf:
                if existing_cs_action.options['name'] == cs_action_conf['name']:
                    found_match = True
                    ensure_cs_action_state(nitro,cs_action_conf)
                    cs_action_conf['existing'] = True

            if not found_match:
                delete_cs_action(nitro,existing_cs_action.options['name'])


    for cs_action_conf in cs_actions_conf:
        if not cs_action_conf.has_key('existing'):
            ensure_cs_action_state(nitro,cs_action_conf)
        else:
            cs_action_conf.pop('existing')

    return ret


def ensure_cs_policies_state(nitro,cs_policies_conf):
    '''

    :param nitro: NSNitro instance
    :param cs_policies_conf: List of Content Switching Policy configuration items from input file
    :return: True if configuration is applied successfully; False otherwise

    Validates NS configuration and applies any changes needed to match the input list of cs_policies configuration items.
    - Iterates through existing CS Policies on the NS to find matches to list of cs_policies config items
    - Config items with matching existing policies on NS are sent to ensure_cs_policy_state()
    - Deletes existing CS policies on the NS with no match in the list of config items
    - Sends items in the config list that do not have an existing match to ensure_cs_policy_state()
   '''
    ret = True
    try:
        all_cs_policies = NSCSPolicy().get_all(nitro)
    except NSNitroError as error:
        pass

    for cs_policy_conf in cs_policies_conf:
        found_match = False
        if all_cs_policies:
            for cs_policy in all_cs_policies:
                if cs_policy.get_policyname() == cs_policy_conf['name']:
                    found_match = True
                    if not ensure_cs_policy_state(nitro,cs_policy_conf):
                        ret = False
                    cs_policy_conf['existing'] = True

                if not found_match:
                    delete_cs_policy(nitro,cs_policy)

    for cs_policy_conf in cs_policies_conf:
        if not cs_policy_conf.has_key('existing'):
           if not ensure_cs_policy_state(nitro,cs_policy_conf):
               ret = False
        else:
            cs_policy_conf.pop('existing')

    return ret

def delete_cs_policy(nitro,cs_policy):
    '''

    :param nitro: NSNitro instance
    :param cs_policy: Existing NSCSPolity object from NS
    :return: True if delete succeeds, False otherwise
    
    - Deletes existing CS Policy from NS
    - Created to work around NSCSPolicy object not deleting properly
    '''
    ret = True
    url = "%s%s/%s" % (nitro.get_url(), cs_policy.resourcetype, cs_policy.get_policyname())
    try:
        response = nitro.delete(url)
    except NSNitroError as error:
        log.debug('delete_cs_policy failed: {0}'.format(error))
        ret = False
    return ret




def ensure_cs_policy_state(nitro,cs_policy_conf):
    '''
    
    :param nitro: NSNitro instance
    :param cs_policy_conf: 
    :return: True if config is applied successfully, False otherwise
    
    - Checks for an existing match to cs_policy config item
    - Updates the existing cs policy on the NS if there is a mismatch with config item
    - With unchangeable properties mismatched, the existing policy is deleted
    - Creates new cs policy on NS if needed
    
    '''
    
    ret = True

    try:
        url = nitro.get_url() + 'cspolicy/' + cs_policy_conf['name']
        response = nitro.get(url).get_response_field('cspolicy')[0]
        if response:
            existing_cs_policy = NSCSPolicy()
            for key in response.keys():
                existing_cs_policy.options[key] = response[key]
    except NSNitroError as error:
        existing_cs_policy = None


    need_new_policy = True
    if existing_cs_policy != None:
        if existing_cs_policy.options['cspolicytype'] == 'Advanced Policy':
            need_new_policy = False
            update = False
            if existing_cs_policy.get_rule() != cs_policy_conf['expression']:
                update = True
                existing_cs_policy.set_rule(cs_policy_conf['expression'])
            if existing_cs_policy.options.has_key('action'):
                if existing_cs_policy.options['action'] != cs_policy_conf['action']:
                    update = True
                    existing_cs_policy.options['action'] = cs_policy_conf['action']
            else:
                update = True
                existing_cs_policy.options['action'] = cs_policy_conf['action']
            if update:
                try:
                    updated_cs_policy = NSCSPolicy()
                    option_names = ['policyname','rule','action']
                    for option_name in option_names:
                        updated_cs_policy.options[option_name] = existing_cs_policy.options[option_name]
                    updated_cs_policy.update_resource(nitro)
                except NSNitroError as error:
                    log.debug('NSCSPolicy.update() failed: {0}'.format(error))
        else:
            delete_cs_policy(nitro,existing_cs_policy)

    if need_new_policy:
        new_cs_policy = NSCSPolicy({'policyname':cs_policy_conf['name'],'rule':cs_policy_conf['expression']})
        new_cs_policy.options['action'] = cs_policy_conf['action']
        try:
            new_cs_policy.add_resource(nitro)
        except NSNitroError as error:
            log.debug('NSCSPolicy.add() failed: {0}'.format(error))


def ensure_csvservers_state(nitro,csvservers_conf):
    '''
    
    :param nitro: NSNitro instance
    :param csvservers_conf: List of csvserver config items from config file 
    :return: True if config is applied successfully, False otherwise
    
    - Sends all csvserver config items to ensure_csvserver_state()
    - Deletes any existing csvservers from NS if it does not have a matching item in the config file
    
    '''
    
    ret = True
    

    for csvserver_conf in csvservers_conf:
        ensure_csvserver_state(nitro,csvserver_conf)

    all_csvservers = NSCSVServer.get_all(nitro)
    csvservers_to_remove = []
    for csvserver in all_csvservers:
        found_match = False
        for csvserver_conf in csvservers_conf:
            if csvserver_conf['name'] == csvserver.get_name():
                found_match = True
        if not found_match:
            csvservers_to_remove.append(csvserver)

    for csvserver_to_remove in csvservers_to_remove:
        try:
            NSCSVServer.delete(nitro, csvserver_to_remove)
        except NSNitroError as error:
            log.debug('NSCSVServer.delete() failed: {0}'.format(error))
            ret = False

    
    return ret


def get_csvserver_lbvserver_binding(nitro, csvserver_name):
    '''
    
    :param nitro: NSNitro instance
    :param csvserver_name: Name of csvserver
    :return: NSBaseResource containing csvserver_lbvserver_binding properties, None if not found on NS
    
    - Implements CRUD capability for csvserver_lbvserver_binding
    - No object for csvserver_lbvserver_binding in NSNitro module
    
    '''
    ret = None
    binding = NSBaseResource()
    binding.resourcetype = 'csvserver_lbvserver_binding'
    try:
        binding.get_resource(nitro,csvserver_name)
        if len(binding.options) != 0:
            ret = binding
    except NSNitroError as error:
        pass
    return ret

def delete_csvserver_lbvserver_binding(nitro, csvserver_name):
    '''
    
    :param nitro: NSNitro instance
    :param csvserver_name: Name of csvserver
    :return: NSBaseResource containing csvserver_lbvserver_binding properties, None if not found on NS
    
    - Implements CRUD capability for csvserver_lbvserver_binding
    - No object for csvserver_lbvserver_binding in NSNitro module
    '''
    ret = False
    binding = NSBaseResource()
    binding.resourcetype = 'csvserver_lbvserver_binding'
    binding.options['name'] = csvserver_name
    try:
        binding.delete_resource(nitro,csvserver_name)
        ret = True
    except NSNitroError as error:
        log.debug('delete_csvserver_lbvserver_binding() failed: {0}'.format(error))
    return ret

def update_csvserver_lbvserver_binding(nitro, csvserver_lbvserver_binding):
    '''
    
    :param nitro: NSNitro instance
    :param csvserver_lbvserver_binding: NSBaseResource mapped to csvserver_lbvserver_binding properties
    :return: True if update successful; False otherwise
    
    - Implements CRUD capability for csvserver_lbvserver_binding
    - No object for csvserver_lbvserver_binding in NSNitro module
    '''
    ret = False
    updated_binding = NSBaseResource()
    updated_binding.resourcetype = 'csvserver_lbvserver_binding'
    for property in ['name','lbvserver']:
        updated_binding.options[property] = csvserver_lbvserver_binding.options[property]
    try:
        updated_binding.update_resource(nitro)
        ret = True
    except NSNitroError as error:
        log.debug('update_csvserver_lbvserver_binding() failed: {0}'.format(error))
    return ret

def add_csvserver_lbvserver_binding(nitro, csvserver_conf):
    '''
    
    :param nitro: NSNitro instance
    :param csvserver_conf: Config item for csvserver from config file 
    :return: True if addedd successfully; False otherwise
    
    - Implements CRUD capability for csvserver_lbvserver_binding
    - No object for csvserver_lbvserver_binding in NSNitro module
    '''
    ret = False
    new_binding = NSBaseResource()
    new_binding.resourcetype = 'csvserver_lbvserver_binding'
    new_binding.options['name'] = csvserver_conf['name']
    new_binding.options['lbvserver'] = csvserver_conf['default_lbvserver']
    try:
        new_binding.add_resource(nitro)
        ret = True
    except NSNitroError as error:
        log.debug('add_csvserver_lbvserver_binding() failed: {0}'.format(error))
    return ret


def ensure_csvserver_state(nitro, csvserver_conf):
    '''
    
    :param nitro: NSNitro instance 
    :param csvserver_conf: Config item for csvserver from config file
    :return: True if state applied successfully; False otherwise
    
    Validates NS configuration and applies any changes needed to match the input csvserver configuration item.
    - Iterates through existing csvservers on the NS to find matching name and/or IP address contained in the csvserver config item
    - An existing csvserver on the NS with a matching name is validated and updated if necessary to match the IP of the csvserver config item
    - An existing csvserver on the NS is deleted if it:
        - Does not have a name that matches the csvserver config item
        - Does have an IP address the matches the csvserver config item
    - Creates an csvserver on the NS using the csvserver config item if one does not exist
    - Iterates through csvserver bindings to service groups in the config item
    - Deletes existing csvserver bindings on the NS that do not match the csvserver config item
    - Creates csvserver binding on the NS for each csvserver config item binding that does not exist
    - Sets csvserver_lbvserver_binding for default_lbvserver value in config item
    
    '''
    ret = True
    
    all_csvservers = NSCSVServer.get_all(nitro)
    matches_found = {}
    for csvserver in all_csvservers:
      if csvserver.get_name() == csvserver_conf['name'] and csvserver.get_ipv46() == csvserver_conf['vip_address'] and csvserver.get_port == csvserver_conf['port'] and csvserver.get_servicetype == csvserver_conf['protocol']:
          matches_found['full_match'] = csvserver
      elif csvserver.get_name() == csvserver_conf['name']:
          matches_found['name'] = csvserver
      elif csvserver.get_ipv46() == csvserver_conf['vip_address']:
          matches_found['vip_address'] = csvserver

    if not matches_found.has_key('full_match'):

        check_for_port_and_protocol_match = False

        if matches_found.has_key('vip_address') and not matches_found.has_key('name'):
            '''
            updated_csvserver = matches_found['vip_address']
            updated_csvserver.set_newname(csvserver_conf['name'])
            try:
                NSCSVServer.rename(nitro, updated_csvserver)
            except NSNitroError as error:
                log.debug('NSCSVServer.update() failed: {0}'.format(error))
                ret = False
            csvserver = NSCSVServer()
            csvserver.set_name(csvserver_conf['name'])
            updated_csvserver = NSCSVServer.get(nitro,csvserver)
            check_for_port_and_protocol_match = True
            '''
            csvserver_to_delete = matches_found['vip_address']
            try:
                NSCSVServer.delete(nitro,csvserver_to_delete)
            except NSNitroError as error:
                log.debug('NSCSVServer.delete() failed: {0}'.format(error))
                ret = False

        if matches_found.has_key('name') and not matches_found.has_key('vip_address'):
            updated_csvserver = matches_found['name']
            updated_csvserver.set_ipv46(csvserver_conf['vip_address'])
            try:
                NSCSVServer.update(nitro, updated_csvserver)
            except NSNitroError as error:
                log.debug('NSCSVServer.update() failed: {0}'.format(error))
                ret = False
            check_for_port_and_protocol_match = True

        if matches_found.has_key('name') and matches_found.has_key('vip_address'):
            update = True
            csvserver_to_delete = matches_found['vip_address']
            try:
                NSCSVServer.delete(nitro,csvserver_to_delete)
            except NSNitroError as error:
                log.debug('NSCSVServer.delete() failed: {0}'.format(error))
                ret = False 
            updated_csvserver = matches_found['name']
            updated_csvserver.set_ipv46(csvserver_conf['vip_address'])
            try:
                NSCSVServer.update(nitro, updated_csvserver)
            except NSNitroError as error:
                log.debug('NSCSVServer.update() failed: {0}'.format(error))
                ret = False
            check_for_port_and_protocol_match = True
        if check_for_port_and_protocol_match:
            update = False
            if updated_csvserver.get_port() != csvserver_conf['port']:
                update = True
                updated_csvserver.set_port(csvserver_conf['port'])
            if updated_csvserver.get_servicetype() != csvserver_conf['protocol']:
                update = True
                updated_csvserver.set_servicetype(csvserver_conf['protocol'])
            if update:
                try:
                    NSCSVServer.update(nitro, updated_csvserver)
                except NSNitroError as error:
                    log.debug('NSCSVServer.update() failed: {0}'.format(error))
                    ret = False

    existing_csvserver = NSCSVServer()
    existing_csvserver.set_name(csvserver_conf['name'])

    try:
        existing_csvserver = NSCSVServer.get(nitro,existing_csvserver)
    except NSNitroError as error:
        log.debug('no existing csvserver found for {}'.format(csvserver_conf['name']))
        existing_csvserver = None

    if existing_csvserver:
        update_needed = False
        delete_needed = False
        if existing_csvserver.get_servicetype() != csvserver_conf['protocol'] or existing_csvserver.get_port() != csvserver_conf['port']:
            delete_needed = True
        elif existing_csvserver.get_ipv46() != csvserver_conf['vip_address']:
            update_needed = True
            existing_csvserver.set_ipv46(csvserver_conf['vip_address'])
        if delete_needed:
            try:
                NSCSVServer.delete(nitro, existing_csvserver)
            except NSNitroError as error:
                log.debug('NSCSVServer.delete() failed: {0}'.format(error))
                ret = False
            existing_csvserver = None
        elif update_needed:
            try:
                NSCSVServer.update(nitro, existing_csvserver)
            except NSNitroError as error:
                log.debug('NSCSVServer.update() failed: {0}'.format(error))
                ret = False

    if not existing_csvserver:
        new_csvserver = NSCSVServer()
        new_csvserver.set_name(csvserver_conf['name'])
        new_csvserver.set_ipv46(csvserver_conf['vip_address'])
        new_csvserver.set_port(csvserver_conf['port'])
        new_csvserver.set_servicetype(csvserver_conf['protocol'])
        try:
            NSCSVServer.add(nitro, new_csvserver)
        except NSNitroError as error:
            log.debug('NSCSVServer.add() failed: {0}'.format(error))
            ret = False
        current_csvserver= new_csvserver

    existing_csvserver_lbvserver_binding = get_csvserver_lbvserver_binding(nitro,csvserver_conf['name'])
    if csvserver_conf.has_key('default_lbvserver'):
        if existing_csvserver_lbvserver_binding != None:
            if existing_csvserver_lbvserver_binding.options['lbvserver'] != csvserver_conf['default_lbvserver']:
                existing_csvserver_lbvserver_binding.options['lbvserver'] = csvserver_conf['default_lbvserver']
                update_csvserver_lbvserver_binding(nitro,existing_csvserver_lbvserver_binding)
        else:
            add_csvserver_lbvserver_binding(nitro,csvserver_conf)
    elif existing_csvserver_lbvserver_binding != None:
        delete_csvserver_lbvserver_binding(nitro,csvserver_conf['name'])


    csvserver_policy_binding = NSCSVServerCSPolicyBinding()
    csvserver_policy_binding.set_name(csvserver_conf['name'])
    try:
        existing_bindings = NSCSVServerCSPolicyBinding.get(nitro, csvserver_policy_binding)
    except NSNitroError as error:
        log.debug('no existing csvserver policy bindings found for {}'.format(csvserver_conf['name']))
        existing_bindings = None

    for binding in csvserver_conf['policy_bindings']:
        if existing_bindings != None:
            bindings_to_remove = []
            for existing_binding in existing_bindings:
                existing_binding_found = False
                if existing_binding.get_policyname() == binding['name']:
                    if existing_binding.get_priority() == binding['priority']:
                        existing_binding_found = True
                        binding['existing'] = True
                if not existing_binding_found:
                    binding_to_remove = existing_binding
                    try:
                        NSCSVServerCSPolicyBinding.delete(nitro, binding_to_remove)
                    except NSNitroError as error:
                        log.debug('NSCSVServerCSPolicyBinding.delete() failed: {0}'.format(error))
                        ret = False

        for binding in csvserver_conf['policy_bindings']:
            if not binding.has_key('existing'):
                new_binding = NSCSVServerCSPolicyBinding()
                new_binding.set_name(csvserver_conf['name'])
                new_binding.set_policyname(binding['name'])
                new_binding.set_priority(binding['priority'])
                try:
                    NSCSVServerCSPolicyBinding.add(nitro, new_binding)
                except NSNitroError as error:
                    log.debug('NSCSVServerCSPolicyBinding.add() failed: {0}'.format(error))
                    ret = False

    
    return ret

def validate_config_yaml(config_from_yaml):
    '''

    :param config_from_yaml: Dictionary containing contents of YAML state declaration
    :return: True if validation successful; False otherwise

    - Validates schema of configuration items from config file
    - Ensures referential integrity of config items that refer to other config items
    '''

    ret = True

    schema = {}
    schema['ns_groups'] = Schema([{'name':str,Optional('ns_instance'):object,Optional('service_groups'):object,
                                   Optional('servers'):object, Optional('lbvservers'):object,
                                   Optional('csvservers'):object, Optional('cs_policies'):object,
                                   Optional('cs_actions'):object}])
    schema['ns_instance'] = Schema({'user': str,
                                    'pass': str,
                                    'address': Or(And(Use(str), lambda n: socket.inet_aton(n)),And(Use(str), lambda n: socket.gethostbyname(n))) })
    schema['service_groups'] = Schema([{'name': str,
                                        'protocol': str,
                                        'servers':[
                                            {'name': And(Use(str),lambda n: n in all_servers),
                                             'port': And(Use(int), lambda n: 1 <= n <= 65535)}]}])
    schema['servers'] = Schema([{'name': str,
                                 'ip_address': And(Use(str), lambda n: socket.inet_aton(n))}])

    schema['lbvservers'] = Schema([{'name': str,
                                    'vip_address': And(Use(str),
                                                       lambda n: socket.inet_aton(n)),
                                    'port':And(Use(int), lambda n: 1 <= n <= 65535),
                                    'protocol': str,
                                    'service_group_bindings':[
                                        And(Use(str),lambda n: n in all_service_groups)
                                    ]}])

    schema['csvservers'] = Schema([{'name': str,
                                    'vip_address': And(Use(str),
                                                       lambda n: socket.inet_aton(n)),
                                    'port':And(Use(int), lambda n: 1 <= n <= 65535),
                                    'protocol': str,
                                    'default_lbvserver': And(Use(str),lambda n: n in all_lbvservers),
                                    'policy_bindings':[
                                        {'name': And(Use(str),lambda n: n in all_cs_policies),
                                        'priority': int}]
                                     }])

    schema['cs_policies'] = Schema([{'name':str, 'expression': str, 'action':And(Use(str),lambda n: n in all_cs_actions)}])

    schema['cs_actions'] = Schema([{'name': str, 'target_lbvserver': And(Use(str),lambda n: n in all_lbvservers)}])


    if 'ns_groups' in config_from_yaml.keys():
        if validate_schema(schema['ns_groups'],config_from_yaml['ns_groups']):
            conf_items = ['ns_instance','service_groups','servers','lbvservers','csvservers','cs_policies','cs_actions']
            for group in config_from_yaml['ns_groups']:
                all_servers = []
                if group.has_key('servers'):
                    for server in group['servers']:
                        if server.has_key('name'):
                            all_servers.append(server['name'])
                all_service_groups = []
                if group.has_key('service_groups'):
                    for service_group in group['service_groups']:
                        if service_group.has_key('name'):
                            all_service_groups.append(service_group['name'])
                all_lbvservers = []
                if group.has_key('lbvservers'):
                    for lbvserver in group['lbvservers']:
                        if lbvserver.has_key('name'):
                            all_lbvservers.append(lbvserver['name'])
                all_cs_policies = []
                if group.has_key('cs_policies'):
                    for cs_policy in group['cs_policies']:
                        if cs_policy.has_key('name'):
                            all_cs_policies.append(cs_policy['name'])
                all_cs_actions = []
                if group.has_key('cs_actions'):
                    for cs_action in group['cs_actions']:
                        if cs_action.has_key('name'):
                            all_cs_actions.append(cs_action['name'])

                for conf_item in conf_items:
                    if group.has_key(conf_item):
                        if validate_schema(schema[conf_item],group[conf_item]) != None:
                            log.info('validation of {} in ns_groups: {} failed'.format(conf_item,group['name']))
                            ret = False
            else:
                log.info('yaml format error')
                ret = False
    else:
        log.info('yaml format error')
        ret = False

    return ret


def validate_schema(schema_obj,input):
    '''

    :param schema_obj: schema.Schema() object
    :param input: object to be validated against schema
    :return: None if schema validation successful; Error message (str) if validation fails

    '''
    ret = None
    try:
        schema_obj.validate(input)
    except SchemaError as error:
        ret = error.message
    return ret


def validate_ns_groups_conf(conf):
    ret = True
    schema = Schema({'ns_groups':[{'name':str,'ns_instance':object}]})
    try:
        schema_obj.validate(input)
    except SchemaError as error:
        log.info('validation of {} in ns_groups: {} failed'.format('ns_instance'))
        ret = False
    return ret

def check_populate_ns_group_yaml(ns_group_conf):
    ret = True

    if not 'name' in ns_group_conf.keys():
        expected_num_keys = 1
    else:
        expected_num_keys = 2

    if expected_num_keys == len(ns_group_conf.keys()):
        received_valid_input = False
        build_yaml = False
        while not received_valid_input:
            user_input = raw_input('Empty NS configuration found in YAML file.  Build YAML from NS? [Y/n]: ')
            if user_input.lower() in ['y','n','']:
                received_valid_input = True
                if user_input.lower() in ['','y']:
                    build_yaml = True
        if build_yaml:
            schema = Schema({'user': str, 'pass': str,
                             'address': Or(And(Use(str), lambda n: socket.inet_aton(n)),And(Use(str), lambda n: socket.gethostbyname(n))) })

            try:
                schema.validate(ns_group_conf['ns_instance'])
                ns_group_conf['build'] = True
            except SchemaError as error:
                log.info('validation of {} in ns_group: {} failed'.format('ns_instance',ns_group_conf['name']))
                ret = False
    return ret

ns_resource_id = {'server':'name',
                'csaction':'name',
                'cspolicy':'policyname',
                'lbvserver':'name',
                'servicegroup':'servicegroupname'
                }

ns_group_resource_types = ['name','ns_instance','servers','service_groups','lbvservers','csvservers','cs_policies','cs_actions']

rw_properties = {'server':
                     [{'nitro':'ipaddress',
                      'yaml':'ip_address'},
                      {'nitro':'name',
                       'yaml':'name'}],
                 'csaction':
                     [{'nitro':'name',
                       'yaml':'name'},
                      {'nitro':'targetlbvserver',
                       'yaml':'target_lbvserver'}],
                 'cspolicy':
                     [{'nitro':'policyname',
                       'yaml':'name'},
                      {'nitro':'rule',
                       'yaml':'expression'},
                      {'nitro':'action',
                       'yaml':'action'}],
                 'lbvserver':
                     [{'nitro':'name',
                       'yaml':'name'},
                      {'nitro':'ipv46',
                       'yaml':'vip_address'},
                      {'nitro':'port',
                       'yaml':'port'},
                      {'nitro':'servicetype',
                       'yaml':'protocol'}],
                 'csvserver':
                     [{'nitro':'name',
                       'yaml':'name'},
                      {'nitro':'ipv46',
                       'yaml':'vip_address'},
                      {'nitro':'port',
                       'yaml':'port'},
                      {'nitro':'servicetype',
                       'yaml':'protocol'}],
                 'servicegroup':
                     [{'nitro':'servicegroupname',
                      'yaml':'name'},
                      {'nitro':'servicetype',
                       'yaml':'protocol'}],
                 'servicegroup_binding':
                     [{'nitro':'servicegroupname',
                       'yaml':'name'},
                      {'nitro':'servername',
                       'yaml':'server'},
                      {'nitro':'port',
                       'yaml':'port'}]
                 }

def convert_list_of_nitro_objects_to_yaml_config(list_of_nitro_objects):
    return_list = []

    if list_of_nitro_objects != None:
        for nitro_object in list_of_nitro_objects:
            return_list.append(map_nitro_object_options_to_yaml_config(nitro_object.resourcetype,nitro_object))

    return return_list

def assign_if_list_not_empty(dict_obj,key_name,value_list):
    if len(value_list) > 0:
        dict_obj[key_name] = value_list

def get_ns_group_conf_from_ns(nitro,input_ns_group_conf):
    ns_group_conf = OrderedDict()
    if 'name' in input_ns_group_conf.keys():
        ns_group_conf['name'] = input_ns_group_conf['name']
    ns_group_conf['ns_instance'] = input_ns_group_conf['ns_instance']
    assign_if_list_not_empty(ns_group_conf, 'servers', convert_list_of_nitro_objects_to_yaml_config(get_all_resources_by_type(nitro,'server')))
    assign_if_list_not_empty(ns_group_conf, 'cs_actions', convert_list_of_nitro_objects_to_yaml_config(get_all_resources_by_type(nitro,'csaction')))
    assign_if_list_not_empty(ns_group_conf, 'lbvservers', convert_list_of_nitro_objects_to_yaml_config(get_all_resources_by_type(nitro,'lbvserver')))
    assign_if_list_not_empty(ns_group_conf, 'csvservers', convert_list_of_nitro_objects_to_yaml_config(get_all_resources_by_type(nitro,'csvserver')))
    assign_if_list_not_empty(ns_group_conf, 'service_groups', convert_list_of_nitro_objects_to_yaml_config(get_all_resources_by_type(nitro,'servicegroup')))
    assign_if_list_not_empty(ns_group_conf, 'cs_policies', convert_list_of_nitro_objects_to_yaml_config(get_all_resources_by_type(nitro,'cspolicy')))

    if 'lbvservers' in ns_group_conf.keys():
        for lbvserver in ns_group_conf['lbvservers']:
            bindings = get_bindings_for_lbvserver(nitro,lbvserver)
            if len(bindings) > 0:
                lbvserver['service_group_bindings'] = bindings

    if 'service_groups' in ns_group_conf.keys():
        for service_group in ns_group_conf['service_groups']:
            bindings = get_bindings_for_service_group(nitro,service_group)
            if len(bindings) > 0:
                service_group['servers'] = bindings

    if 'csvservers' in ns_group_conf.keys():
        for csvserver in ns_group_conf['csvservers']:
            bindings = get_policy_bindings_for_csvserver(nitro,csvserver)
            if len(bindings) > 0:
                csvserver['policy_bindings'] = bindings
            target_lbvserver = get_resource_by_type_and_name(nitro,'csvserver_lbvserver_binding',csvserver['name'])
            if target_lbvserver != None:
                csvserver['target_lbvserver'] = target_lbvserver[0].options['lbvserver']

    return ns_group_conf

def get_resource_by_type_and_name(nitro,resource_type,resource_name):
    matching_resources = []
    url = nitro.get_url() + resource_type + '/' + resource_name
    try:
        response = nitro.get(url).get_response_field(resource_type)
        if type(response) == list:
            for item in response:
                resource = NSBaseResource()
                resource.resourcetype = resource_type
                resource.options = item
                matching_resources.append(resource)
        else:
            resource = NSBaseResource()
            resource.resourcetype = resource_type
            resource.options = item
            matching_resources.append(resource)
    except NSNitroError as error:
        log.debug('no {} resources found on ns'.format(resource_type))
        matching_resources = None
    return matching_resources

def get_bindings_for_lbvserver(nitro,lbvserver_conf):
    returned_bindings = []
    resource_type = 'lbvserver_servicegroup_binding'
    bindings = get_resource_by_type_and_name(nitro,resource_type,lbvserver_conf['name'])
    if bindings != None:
        for binding in bindings:
            returned_bindings.append(binding.options['servicegroupname'])
    return returned_bindings


def get_policy_bindings_for_csvserver(nitro,csvserver_conf):
    returned_bindings = []
    resource_type = 'cspolicy_csvserver_binding'
    bindings = get_resource_by_type_and_name(nitro,resource_type,csvserver_conf['name'])
    if bindings != None:
        for binding in bindings:
            returned_bindings.append({'name':binding.options['policyname'],'priority':int(binding.options['priority'])})
    return returned_bindings


def get_lbvserver_bindings_for_csvserver(nitro,csvserver_conf):
    returned_bindings = []
    resource_type = 'csvserver_lbvserver_binding'
    bindings = get_resource_by_type_and_name(nitro,resource_type,csvserver_conf['name'])
    if bindings != None:
        for binding in bindings:
            returned_bindings.append(binding.options['lbvserver'])
    return returned_bindings

def build_servers_conf_from_ns(nitro):
    conf = []
    all_items = NSServer.get_all(nitro)
    for item in all_items:
        conf.append(item.options)
    return conf

def build_cs_action_conf_from_ns(nitro):
    conf = []
    all_items = get_all_cs_actions(nitro)
    for item in all_items:
        conf.append(item.options)
    return conf

def get_all_resources_by_type(nitro,resource_type):
    '''
    :param nitro: NSNitro object
    :return: list of csaction objects
    Creates custom url string to get all csaction objects from the Netscaler.  Copies name and targetlbvserver options
    to empty csaction objects to avoid update issues with read-only options present.
    '''
    from inspect import getmembers
    all_resources = []
    #resource_type = 'csaction'
    url = nitro.get_url() + resource_type
    try:
        resources = nitro.get(url).get_response_field(resource_type)
    except NSNitroError as error:
        log.debug('no {} resources found on ns'.format(resource_type))
        resources = None
    if resources != None:
        for resource in resources:
            new_resource = NSBaseResource()
            new_resource.resourcetype = resource_type
            new_resource.set_options(resource)
            all_resources.append(new_resource)
    return all_resources


def get_bindings_for_service_group(nitro, service_group_conf):
    returned_bindings = []
    resource_type = 'servicegroup_servicegroupmember_binding'
    bindings = get_resource_by_type_and_name(nitro,resource_type,service_group_conf['name'])
    if bindings != None:
        for binding in bindings:
            returned_bindings.append({'name':binding.options['servername'],'port':binding.options['port']})
    return returned_bindings


def connect_nitro (ns_instance_conf):
    nitro = nitro_service(ns_instance_conf['address'],'http')
    try:
        nitro.login(ns_instance_conf['user'],ns_instance_conf['pass'],1800)
        ret = nitro
    except nitro_exception as error:
        log.info('connection to {} failed'.format(ns_instance_conf['address']))
        ret = False
    return ret

def disconnect_nitro (nitro):
    try:
        nitro.logout()
        ret = True
    except nitro_exception as error:
        log.info('logout from {} failed'.format(nitro.__getattribute__('ipaddress')))
        ret = False


def create_ordered_dict_from_config_yaml(config_yaml):
    return config_yaml
    ordered_config = {}
    ordered_config['ns_groups'] = []
    for ns_group in config_yaml['ns_groups']:
        ordered_ns_group_config = {}
        for resource_type in ns_group_resource_types:
            if resource_type in ns_group.keys():
                ordered_ns_group_config[resource_type] = ns_group[resource_type]
        ordered_config['ns_groups'].append(ordered_ns_group_config)

    return ordered_config

def main():
    log.info('Using config file: {}'.format(sys.argv[1]))
    conf = get_config_yaml(sys.argv[1])

    need_yaml_update = False

    if validate_config_yaml(conf):
        conf = create_ordered_dict_from_config_yaml(conf)
        backup_config = {}
        backup_config['ns_groups'] = []
        for ns_group in conf['ns_groups']:
            log.info('Processing group {}'.format(ns_group['name']))

            ns_instance = ns_group['ns_instance']
            nitro = connect(ns_instance)
            #nitro = connect_nitro(ns_instance)
            if nitro.get_sessionid() != None:
                backup_ns_group_conf = get_ns_group_conf_from_ns(nitro,ns_group)
                backup_config['ns_groups'].append(backup_ns_group_conf)
                if check_populate_ns_group_yaml(ns_group):
                    need_yaml_update = True
                if 'build' in ns_group.keys():
                    for key in backup_ns_group_conf.keys():
                        ns_group[key] = backup_ns_group_conf[key]
                    ns_group.pop('build')
                else:
                    if 'servers' in ns_group.keys():
                        ensure_servers_state(nitro,ns_group['servers'])
                    if 'service_groups' in ns_group.keys():
                        ensure_service_groups_state(nitro,ns_group['service_groups'])
                    if 'lbvservers' in ns_group.keys():
                        ensure_lbvservers_state(nitro,ns_group['lbvservers'])
                    if 'cs_actions' in ns_group.keys():
                        ensure_cs_actions_state(nitro,ns_group['cs_actions'])
                    if 'cs_policies' in ns_group.keys():
                        ensure_cs_policies_state(nitro,ns_group['cs_policies'])
                    if 'csvservers' in ns_group.keys():
                        ensure_csvservers_state(nitro,ns_group['csvservers'])
                disconnect(nitro)
                #disconnect_nitro(nitro)
            else:
                log.info('Connection to NetScaler on {} failed'.format(ns_group['ns_instance']['address']))
        if need_yaml_update:
            update_yaml(create_ordered_dict_from_config_yaml(conf),sys.argv[1])
        update_yaml(create_ordered_dict_from_config_yaml(backup_config),'out.yml')
                    #'backup_ns_config_{}.yml'.format(strftime("%Y%m%d_%H%M%S")))


if __name__ == "__main__": main()

