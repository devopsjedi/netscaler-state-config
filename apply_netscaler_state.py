
import sys
import yaml
import logging
from schema import Schema, And, Use, Or, Optional, SchemaError
import socket
from time import strftime
from collections import OrderedDict
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

log_filename = 'apply_netscaler_state_{}.log'.format(strftime("%Y%m%d_%H%M%S"))
log = logging.getLogger('apply_netscaler_state')
log.setLevel(logging.DEBUG)
stream = logging.StreamHandler()
stream.setLevel(logging.DEBUG)
file = logging.FileHandler(log_filename)
log.addHandler(stream)
log.addHandler(file)


ns_resource_id = {'server':'name',
                'csaction':'name',
                'cspolicy':'policyname',
                'lbvserver':'name',
                'servicegroup':'servicegroupname'
                }

yaml_config_resource_types = ['servers','service_groups','lbvservers','csvservers','cs_actions','cs_policies']

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
                 'servicegroup_servicegroupmember_binding':
                     [{'nitro':'servername',
                       'yaml':'name'},
                      {'nitro':'port',
                       'yaml':'port'}],
                 'cspolicy_csvserver_binding':
                     [{'nitro':'policyname',
                       'yaml':'name'},
                      {'nitro':'priority',
                       'yaml':'priority'}]
                 }


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
    '''

    :param conf: Dictionary containing YAML-based configuration
    :param filename: Filename of YAML config file
    :return: True if export succeeds; False otherwise
    '''
    ret = False
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

    return ret


def ordered_load(stream, Loader=yaml.Loader, object_pairs_hook=OrderedDict):
    '''

    :param stream: stream object for file input
    :param Loader: yaml.Loader class
    :param object_pairs_hook: assocated with OrderedDict
    :return:

    Needed a method to load YAML as an OrderedDict object so input file format could be maintained during export.
    Found and used solution from this URL: http://ambracode.com/index/show/13055

    '''
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
    '''

    :param data: Input data object to be exported
    :param stream: stream object for file access
    :param Dumper: yaml.Dumper class
    :param kwds: keywords passed to yaml dumper
    :return: yaml.dump return value

    Needed a method to export YAML from an OrderedDict object so existing resource configuration order could be maintained during export.
    Found and used solution from this URL: http://ambracode.com/index/show/13055
    kwargs: default_flow_style=False produces one line for each entity

    '''
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

    if 'full_match' in matches_found.keys():
        return True
    else:
        update = False
        if 'ip_address' in matches_found.keys() and not 'name' in matches_found.keys():
            server_to_delete = matches_found['ip_address']
            try:
                NSServer.delete(nitro,server_to_delete)
            except NSNitroError as error:
                log.debug('NSServer.delete() failed: {0}'.format(error))
                ret = False
        if 'name' in matches_found.keys() and not 'ip_address' in matches_found.keys():
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
        if 'name' in matches_found.keys() and 'ip_address' in matches_found.keys():
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

    #existing_bindings = get_bindings_for_service_group(nitro,service_group_conf)
    existing_bindings = get_all_resources_by_type_and_name(nitro,'servicegroup_servicegroupmember_binding',service_group_conf['name'])

    if existing_bindings is not None:
        bindings_to_remove = []
        for existing_binding in existing_bindings:
            binding_found = False
            for server_binding_conf in service_group_conf['servers']:
                existing_conf = map_nitro_object_options_to_yaml_config('servicegroup_servicegroupmember_binding',existing_binding)
                if  existing_conf == server_binding_conf:
                    binding_found = True
                    server_binding_conf['bound'] = True
            if not binding_found:
                if 'ip' in existing_binding.options.keys():
                    existing_binding.options.pop('ip')
                binding_to_remove = NSServiceGroupServerBinding(existing_binding.options)
                try:
                    NSServiceGroupServerBinding.delete(nitro, binding_to_remove)
                except NSNitroError as error:
                    log.debug('NSServiceGroupServerBinding.remove() failed: {0}'.format(error))
                    ret = False


        for server_binding_conf in service_group_conf['servers']:
            if not 'bound' in server_binding_conf.keys():
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
    if service_groups_conf is not None:
        for service_group_conf in service_groups_conf:
            ensure_service_group_state(nitro,service_group_conf)
        remove_all_existing_service_groups = False
    else:
        remove_all_existing_service_groups = True

    all_service_groups = NSServiceGroup.get_all(nitro)
    service_groups_to_remove = []
    for service_group in all_service_groups:
        found_match = False
        if not remove_all_existing_service_groups:
            for service_group_conf in service_groups_conf:
                if service_group_conf['name'] == service_group.get_servicegroupname():
                    found_match = True
        if not found_match:
            service_groups_to_remove.append(service_group)

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
    if servers_conf is not None:
        for server_conf in servers_conf:
            ensure_server_state(nitro,server_conf)
        remove_all_existing_servers = False
    else:
        remove_all_existing_servers = True

    all_servers = NSServer.get_all(nitro)
    servers_to_remove = []
    for server in all_servers:
        found_match = False
        if not remove_all_existing_servers:
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
    if lbvservers_conf is not None:
        for lbvserver_conf in lbvservers_conf:
            ensure_lbvserver_state(nitro,lbvserver_conf)
        remove_all_existing_lbvservers = False
    else:
        remove_all_existing_lbvservers = True

    all_lbvservers = NSLBVServer.get_all(nitro)
    lbvservers_to_remove = []
    for lbvserver in all_lbvservers:
        found_match = False
        if not remove_all_existing_lbvservers:
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

    if not 'full_match' in matches_found.keys():

        check_for_port_and_protocol_match = False

        if 'vip_address' in matches_found.keys() and not 'name' in matches_found.keys():
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

        if 'name' in matches_found.keys() and not 'vip_address' in matches_found.keys():
            updated_lbvserver = matches_found['name']
            updated_lbvserver.set_ipv46(lbvserver_conf['vip_address'])
            try:
                NSLBVServer.update(nitro, updated_lbvserver)
            except NSNitroError as error:
                log.debug('NSLBVServer.update() failed: {0}'.format(error))
                ret = False
            check_for_port_and_protocol_match = True

        if 'name' in matches_found.keys() and 'vip_address' in matches_found.keys():
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

    if bindings is not None:
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
        if lbvserver_service_group_bindings[lbvserver_service_group_binding] is None:
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
    if cs_actions_conf is not None:
        for cs_action_conf in cs_actions_conf:
            ensure_cs_action_state(nitro,cs_action_conf)
        remove_all_existing_cs_actions = False
    else:
        remove_all_existing_cs_actions = True

    all_cs_actions = get_all_cs_actions(nitro)

    if all_cs_actions is not None:
        for existing_cs_action in all_cs_actions:
            found_match = False
            if not remove_all_existing_cs_actions:
                for cs_action_conf in cs_actions_conf:
                    if existing_cs_action.options['name'] == cs_action_conf['name']:
                        found_match = True

            if not found_match:
                delete_cs_action(nitro,existing_cs_action.options['name'])

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

    if cs_policies_conf is not None:
        for cs_policy_conf in cs_policies_conf:
            ensure_cs_policy_state(nitro,cs_policy_conf)
        remove_all_existing_cs_policies = False
    else:
        remove_all_existing_cs_policies = True

    try:
        all_cs_policies = NSCSPolicy().get_all(nitro)
    except NSNitroError as error:
        all_cs_policies = None

    if all_cs_policies is not None:
        for existing_cs_policy in all_cs_policies:
            found_match = False
            if not remove_all_existing_cs_policies:
                for cs_policy_conf in cs_policies_conf:
                    if existing_cs_policy.get_policyname() == cs_policy_conf['name']:
                        found_match = True
            if not found_match:
                delete_cs_policy(nitro,existing_cs_policy)

    else:
        remove_all_existing_cs_policies = True

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
    if existing_cs_policy is not None:
        if existing_cs_policy.options['cspolicytype'] == 'Advanced Policy':
            need_new_policy = False
            update = False
            if existing_cs_policy.get_rule() != cs_policy_conf['expression']:
                update = True
                existing_cs_policy.set_rule(cs_policy_conf['expression'])
            if 'action' in existing_cs_policy.options.keys():
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
    
    if csvservers_conf is not None:
        for csvserver_conf in csvservers_conf:
            ensure_csvserver_state(nitro,csvserver_conf)
        remove_all_existing_csvservers = False
    else:
        remove_all_existing_csvservers = True

    all_csvservers = NSCSVServer.get_all(nitro)
    csvservers_to_remove = []

    for csvserver in all_csvservers:
        found_match = False
        if not remove_all_existing_csvservers:
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

    if not 'full_match' in matches_found.keys():

        check_for_port_and_protocol_match = False

        if 'vip_address' in matches_found.keys() and not 'name' in matches_found.keys():
            csvserver_to_delete = matches_found['vip_address']
            try:
                NSCSVServer.delete(nitro,csvserver_to_delete)
            except NSNitroError as error:
                log.debug('NSCSVServer.delete() failed: {0}'.format(error))
                ret = False

        if 'name' in matches_found.keys() and not 'vip_address' in matches_found.keys():
            updated_csvserver = matches_found['name']
            updated_csvserver.set_ipv46(csvserver_conf['vip_address'])
            try:
                NSCSVServer.update(nitro, updated_csvserver)
            except NSNitroError as error:
                log.debug('NSCSVServer.update() failed: {0}'.format(error))
                ret = False
            check_for_port_and_protocol_match = True

        if 'name' in matches_found.keys() and 'vip_address' in matches_found.keys():
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

    existing_csvserver_lbvserver_binding = get_csvserver_lbvserver_binding(nitro,csvserver_conf['name'])
    if 'default_lbvserver' in csvserver_conf.keys():
        if existing_csvserver_lbvserver_binding is not None:
            if existing_csvserver_lbvserver_binding.options['lbvserver'] != csvserver_conf['default_lbvserver']:
                existing_csvserver_lbvserver_binding.options['lbvserver'] = csvserver_conf['default_lbvserver']
                update_csvserver_lbvserver_binding(nitro,existing_csvserver_lbvserver_binding)
        else:
            add_csvserver_lbvserver_binding(nitro,csvserver_conf)
    elif existing_csvserver_lbvserver_binding is not None:
        delete_csvserver_lbvserver_binding(nitro,csvserver_conf['name'])

    csvserver_policy_binding = NSCSVServerCSPolicyBinding()
    csvserver_policy_binding.set_name(csvserver_conf['name'])
    try:
        existing_bindings = NSCSVServerCSPolicyBinding.get(nitro, csvserver_policy_binding)
    except NSNitroError as error:
        log.debug('no existing csvserver policy bindings found for {}'.format(csvserver_conf['name']))
        existing_bindings = None

    for binding in csvserver_conf['policy_bindings']:
        if existing_bindings is not None:
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
            if not 'existing' in binding.keys():
                new_binding = NSCSVServerCSPolicyBinding()
                new_binding.set_name(csvserver_conf['name'])
                new_binding.set_policyname(binding['name'])
                new_binding.set_priority(binding['priority'])
                try:
                    NSCSVServerCSPolicyBinding.add(nitro, new_binding)
                except NSNitroError as error:
                    log.debug('NSCSVServerCSPolicyBinding.add() failed: {0}'.format(error))
                    ret = False
            else:
                binding.pop('existing')

    
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
                                   Optional('cs_actions'):object, Optional('build'):object}])
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
                if 'servers' in group.keys():
                    for server in group['servers']:
                        if 'name' in server.keys():
                            all_servers.append(server['name'])
                all_service_groups = []
                if 'service_groups' in group.keys():
                    for service_group in group['service_groups']:
                        if 'name' in service_group.keys():
                            all_service_groups.append(service_group['name'])
                all_lbvservers = []
                if 'lbvservers' in group.keys():
                    for lbvserver in group['lbvservers']:
                        if 'name' in lbvserver.keys():
                            all_lbvservers.append(lbvserver['name'])
                all_cs_policies = []
                if 'cs_policies' in group.keys():
                    for cs_policy in group['cs_policies']:
                        if 'name' in cs_policy.keys():
                            all_cs_policies.append(cs_policy['name'])
                all_cs_actions = []
                if 'cs_actions' in group.keys():
                    for cs_action in group['cs_actions']:
                        if 'name' in cs_action.keys():
                            all_cs_actions.append(cs_action['name'])

                for conf_item in conf_items:
                    if conf_item in group.keys():
                        if validate_schema(schema[conf_item],group[conf_item]) is not None:
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
    '''

    :param conf:  Dictionary containing contents of YAML state declaration
    :return:    True if schema is valid; False otherwise

    Validates top level of YAML configuration file to ensure the correct ns_groups declaration format

    '''
    ret = True
    schema = Schema({'ns_groups':[{'name':str,'ns_instance':object}]})
    try:
        schema_obj.validate(input)
    except SchemaError as error:
        log.info('validation of {} in ns_groups: {} failed'.format('ns_instance'))
        ret = False
    return ret


def check_populate_ns_group_yaml(ns_group_conf):
    '''
    :param ns_group_conf:   Dictionary containing contents of ns_group YAML state declaration
    :return:  True if YAML format is valid; False otherwise

    Checks the ns_group configuration object to see if it contains any resources.  If not, the user is prompted to
    allow the input file to be updated with the current NetScaler configuration.  This function creates the capability
    for a user to automatically build a config YAML from an existing NetScaler configuration.

    '''
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
    return None


def convert_list_of_nitro_objects_to_yaml_config(list_of_nitro_objects):
    '''

    :param list_of_nitro_objects: List containing NSBaseResource objects
    :return: List of YAML configuration objects corresponding to list of input objects

    Maps object attributes from NSNitro-based objects to YAML-based configuration syntax

    '''
    return_list = []

    if list_of_nitro_objects is not None:
        for nitro_object in list_of_nitro_objects:
            return_list.append(map_nitro_object_options_to_yaml_config(nitro_object.resourcetype,nitro_object))

    return return_list


def assign_if_list_not_empty(dict_obj,key_name,value_list):
    '''

    :param dict_obj: Dictionary
    :param key_name: Key name to assign
    :param value_list: Input list to be assigned if not empty
    :return: None

    If input list is not empty, it is assigned to the input dictionary as the designated key name's value

    '''
    if len(value_list) > 0:
        dict_obj[key_name] = value_list

    return None


def get_ns_group_conf_from_ns(nitro,input_ns_group_conf):
    '''

    :param nitro: NSNitro connection object
    :param input_ns_group_conf: Configuration dictionary representing an ns_group
    :return: OrderedDict object with the configuration obtained from the NetScaler

    Queries the NetScaler for the existing configuration and builds a corresponding YAML-based configuration.

    '''
    ns_group_conf = OrderedDict()
    if 'name' in input_ns_group_conf.keys():
        ns_group_conf['name'] = input_ns_group_conf['name']
    ns_group_conf['ns_instance'] = input_ns_group_conf['ns_instance']
    assign_if_list_not_empty(ns_group_conf, 'servers', convert_list_of_nitro_objects_to_yaml_config(get_all_resources_by_type(nitro,'server')))
    assign_if_list_not_empty(ns_group_conf, 'service_groups', convert_list_of_nitro_objects_to_yaml_config(get_all_resources_by_type(nitro,'servicegroup')))
    assign_if_list_not_empty(ns_group_conf, 'lbvservers', convert_list_of_nitro_objects_to_yaml_config(get_all_resources_by_type(nitro,'lbvserver')))
    assign_if_list_not_empty(ns_group_conf, 'csvservers', convert_list_of_nitro_objects_to_yaml_config(get_all_resources_by_type(nitro,'csvserver')))
    assign_if_list_not_empty(ns_group_conf, 'cs_policies', convert_list_of_nitro_objects_to_yaml_config(get_all_resources_by_type(nitro,'cspolicy')))
    assign_if_list_not_empty(ns_group_conf, 'cs_actions', convert_list_of_nitro_objects_to_yaml_config(get_all_resources_by_type(nitro,'csaction')))

    if 'lbvservers' in ns_group_conf.keys():
        for lbvserver in ns_group_conf['lbvservers']:
            bindings = get_bindings_for_lbvserver(nitro,lbvserver)
            if len(bindings) > 0:
                lbvserver['service_group_bindings'] = bindings

    if 'service_groups' in ns_group_conf.keys():
        for service_group in ns_group_conf['service_groups']:
            #bindings = get_bindings_for_service_group(nitro,service_group)
            bindings = get_all_resources_by_type_and_name(nitro,'servicegroup_servicegroupmember_binding',service_group['name'])
            if len(bindings) > 0:
                service_group['servers'] = []
                for binding in bindings:
                    service_group['servers'].append(map_nitro_object_options_to_yaml_config('servicegroup_servicegroupmember_binding',binding))

    if 'csvservers' in ns_group_conf.keys():
        for csvserver in ns_group_conf['csvservers']:
            bindings = get_policy_bindings_for_csvserver(nitro,csvserver)
            if len(bindings) > 0:
                csvserver['policy_bindings'] = bindings
            target_lbvserver = get_all_resources_by_type_and_name(nitro,'csvserver_lbvserver_binding',csvserver['name'])
            if target_lbvserver is not None:
                csvserver['target_lbvserver'] = target_lbvserver[0].options['lbvserver']

    return ns_group_conf


def get_all_resources_by_type_and_name(nitro,resource_type,resource_name):
    '''
    :param nitro: NSNitro connection object
    :param resource_type: NetScaler resource name as contained in the Nitro API
    :param resource_name: Name of resource to send in GET request
    :return: List of NSBaseResponse objects returned from request to NetScaler; Empty list of no objects returned
    '''
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
    '''

    :param nitro: NSNitro connection object
    :param lbvserver_conf: Load Balancing Virtual Server object configuration
    :return: List of Service Group names that are bound to the named LBVServer in the input config
    '''
    returned_bindings = []
    resource_type = 'lbvserver_servicegroup_binding'
    bindings = get_all_resources_by_type_and_name(nitro,resource_type,lbvserver_conf['name'])
    if bindings is not None:
        for binding in bindings:
            returned_bindings.append(binding.options['servicegroupname'])
    return returned_bindings


def get_policy_bindings_for_csvserver(nitro,csvserver_conf):
    '''

    :param nitro: NSNitro connection object
    :param csvserver_conf: Content Switching Virtual Server object configuration
    :return: List of CS Policy names that are bound to the named CSVServer in the input config
    '''
    returned_bindings = []
    resource_type = 'cspolicy_csvserver_binding'
    bindings = get_all_resources_by_type_and_name(nitro,resource_type,csvserver_conf['name'])
    if bindings is not None:
        for binding in bindings:
            returned_bindings.append(map_nitro_object_options_to_yaml_config(resource_type,binding))
    return returned_bindings


def get_lbvserver_bindings_for_csvserver(nitro,csvserver_conf):
    '''

    :param nitro: NSNitro connection object
    :param csvserver_conf: Content Switching Virtual Server object configuration
    :return: Name of default LBVServer bound to CSVServer in the input config
    '''
    ret = None
    resource_type = 'csvserver_lbvserver_binding'
    bindings = get_all_resources_by_type_and_name(nitro,resource_type,csvserver_conf['name'])
    if bindings is not None:
        ret = binding.options['lbvserver']
    return ret


def get_all_resources_by_type(nitro,resource_type):
    '''
    :param nitro: NSNitro connection object
    :param resource_type: Name of resource type as defined in the Nitro API
    :return: List of NSBaseResource objects containing the data returned from the NetScaler
    '''

    all_resources = []
    url = nitro.get_url() + resource_type
    try:
        resources = nitro.get(url).get_response_field(resource_type)
    except NSNitroError as error:
        log.debug('no {} resources found on ns'.format(resource_type))
        resources = None
    if resources is not None:
        for resource in resources:
            new_resource = NSBaseResource()
            new_resource.resourcetype = resource_type
            new_resource.set_options(resource)
            all_resources.append(new_resource)
    return all_resources


def get_bindings_for_service_group(nitro, service_group_conf):
    '''
    :param nitro: NSNitro connection object
    :param csvserver_conf: Service Group object configuration
    :return: List of Service Group Server Binding configurations from NetScaler
    '''
    returned_bindings = []
    resource_type = 'servicegroup_servicegroupmember_binding'
    bindings = get_all_resources_by_type_and_name(nitro,resource_type,service_group_conf['name'])
    if bindings is not None:
        for binding in bindings:
            returned_bindings.append(map_nitro_object_options_to_yaml_config(resource_type,binding))
    return returned_bindings


def create_ordered_dict_from_config_yaml(config_yaml):
    '''
    :param config_yaml: Dictionary object containing YAML-based configuration
    :return:  OrderedDict object with keys added in the desired sequence.  Used in formatting of YAML export.
    '''

    ordered_config = OrderedDict()
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
    if conf is not None:
        need_yaml_update = False

        # Validate YAML configuration file; Prevents applying invalid configuration
        if validate_config_yaml(conf):
            backup_config = OrderedDict()
            backup_config['ns_groups'] = []
            for ns_group in conf['ns_groups']:
                log.info('Processing group {}'.format(ns_group['name']))

                ns_instance = ns_group['ns_instance']
                nitro = connect(ns_instance)
                if nitro.get_sessionid() is not None:
                    # Create backup configuration from NetScaler instance
                    backup_ns_group_conf = get_ns_group_conf_from_ns(nitro,ns_group)
                    backup_config['ns_groups'].append(backup_ns_group_conf)
                    check_populate_ns_group_yaml(ns_group)
                    # Check for empty config or the presence of the 'build' flag in the YAML config file
                    if 'build' in ns_group.keys():
                        need_yaml_update = True
                        for key in backup_ns_group_conf.keys():
                            ns_group[key] = backup_ns_group_conf[key]
                        ns_group.pop('build')
                    else:
                        # Iterates through 5 times per NS instance to ensure that dependencies are addressed during state configuration
                        i = 0
                        while i <= 5:
                            # Iterates through YAML config file resource types to apply state for each
                            for yaml_config_resource_type in yaml_config_resource_types:
                                if yaml_config_resource_type in ns_group.keys():
                                    exec('ensure_' + yaml_config_resource_type + '_state(nitro,ns_group[yaml_config_resource_type])')
                                else:
                                    exec('ensure_' + yaml_config_resource_type + '_state(nitro,None)')
                            i += 1
                    disconnect(nitro)
                else:
                    log.info('Connection to NetScaler on {} failed'.format(ns_group['ns_instance']['address']))
            if need_yaml_update:
                # Updates input config file if 'build' option is selected
                update_yaml(conf,sys.argv[1])
            #update_yaml(create_ordered_dict_from_config_yaml(backup_config),'backup.yml')
            update_yaml(backup_config,'backup_ns_config_{}.yml'.format(strftime("%Y%m%d_%H%M%S")))


if __name__ == "__main__": main()

