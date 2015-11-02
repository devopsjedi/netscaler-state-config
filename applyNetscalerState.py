__author__ = 'devopsjedi'

import sys
import yaml
import logging

LOG_FILENAME = 'applyNetscalerState.log'

from nsnitro.nsnitro import NSNitro
from nsnitro.nsexceptions import NSNitroError
from nsnitro.nsresources.nsserver import NSServer
from nsnitro.nsresources.nsservicegroup import NSServiceGroup
from nsnitro.nsresources.nsservicegroupserverbinding import NSServiceGroupServerBinding
from nsnitro.nsresources.nslbvserver import NSLBVServer
from nsnitro.nsresources.nslbvserverservicegroupbinding import NSLBVServerServiceGroupBinding
from nsnitro.nsresources.nsbaseresource import NSBaseResource
#from nsnitro.nsresources.nssslvserversslcertkeybinding import NSSSLVServerSSLCertKeyBinding


log = logging.getLogger('applyNetscalerState')
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
log.addHandler(ch)

def get_config_yaml(filename):
    stream = open(filename,"r")
    conf = yaml.load(stream)
    return conf


def connect(ns_instance):
    nitro = NSNitro(ns_instance['address'],ns_instance['user'],ns_instance['pass'])
    try:
        nitro.login()
    except NSNitroError as error:
        log.debug('netscaler module error - NSNitro.login() failed: {0}'.format(error))
    return nitro


def disconnect(nitro):
    try:
        nitro.logout()
    except NSNitroError as error:
        log.debug('netscaler module error - NSNitro.logout() failed: {0}'.format(error))
        return None
    return nitro


def ensure_server_state(nitro, server_obj):
    ret = True
    
    all_servers = NSServer.get_all(nitro)
    matches_found = {}
    for server in all_servers:
      if server.get_name() == server_obj['name'] and server.get_ipaddress() == server_obj['ip_address']:
          matches_found['full_match'] = server
      elif server.get_name() == server_obj['name']:
          matches_found['name'] = server
      elif server.get_ipaddress() == server_obj['ip_address']:
          matches_found['ip_address'] = server

    if matches_found.has_key('full_match'):
        return True
    else:
        update = False
        if matches_found.has_key('ip_address') and not matches_found.has_key('name'):
            update = True
            updated_server = matches_found['ip_address']
            updated_server.set_newname(server_obj['name'])
            try:
                NSServer.rename(nitro, updated_server)
            except NSNitroError as error:
                log.debug('netscaler module error - NSServer.update() failed: {0}'.format(error))
                ret = False
        if matches_found.has_key('name') and not matches_found.has_key('ip_address'):
            update = True
            updated_server = matches_found['name']
            updated_server.set_ipaddress(server_obj['ip_address'])
            updated_server.set_translationip(None)
            updated_server.set_translationmask(None)
            try:
                NSServer.update(nitro, updated_server)
            except NSNitroError as error:
                log.debug('netscaler module error - NSServer.update() failed: {0}'.format(error))
                ret = False
        if matches_found.has_key('name') and matches_found.has_key('ip_address'):
            update = True
            server_to_delete = matches_found['ip_address']
            try:
                NSServer.delete(nitro,server_to_delete)
            except NSNitroError as error:
                log.debug('netscaler module error - NSServer.delete() failed: {0}'.format(error))
                ret = False 
            updated_server = matches_found['name']
            updated_server.set_ipaddress(server_obj['ip_address'])
            try:
                NSServer.update(nitro, updated_server)
            except NSNitroError as error:
                log.debug('netscaler module error - NSServer.update() failed: {0}'.format(error))
                ret = False
        if not update:
            new_server = NSServer()
            new_server.set_name(server_obj['name'])
            new_server.set_ipaddress(server_obj['ip_address'])
            try:
                NSServer.add(nitro,new_server)
            except NSNitroError as error:
                log.debug('netscaler module error - NSServer.add() failed: {0}'.format(error))
                ret = False

    
    return ret


def ensure_servicegroup_state(nitro, servicegroup_obj):
    ret = True
    
    existing_servicegroup = NSServiceGroup()

    existing_servicegroup.set_servicegroupname(servicegroup_obj['name'])
    try:
        existing_servicegroup = NSServiceGroup.get(nitro,existing_servicegroup)
    except NSNitroError as error:
        log.debug('no existing servicegroup found for {}'.format(servicegroup_obj['name']))
        existing_servicegroup = None

    if existing_servicegroup:
        if existing_servicegroup.get_servicetype() != servicegroup_obj['protocol']:
            try:
                NSServiceGroup.delete(nitro, existing_servicegroup)
            except NSNitroError as error:
                log.debug('netscaler module error - NSServiceGroup.delete() failed: {0}'.format(error))
                ret = False
            existing_servicegroup = None

    if not existing_servicegroup:
        new_servicegroup = NSServiceGroup()
        new_servicegroup.set_servicegroupname(servicegroup_obj['name'])
        new_servicegroup.set_servicetype(servicegroup_obj['protocol'])
        try:
            NSServiceGroup.add(nitro, new_servicegroup)
        except NSNitroError as error:
            log.debug('netscaler module error - NSServiceGroup.add() failed: {0}'.format(error))
            ret = False
        current_servicegroup= new_servicegroup

    servicegroup_binding = NSServiceGroupServerBinding()
    servicegroup_binding.set_servicegroupname(servicegroup_obj['name'])
    try:
        bindings = NSServiceGroupServerBinding.get(nitro, servicegroup_binding)
    except NSNitroError as error:
        log.debug('no existing servicegroup server bindings found for {}'.format(servicegroup_obj['name']))
        bindings = None

    if bindings != None:
        bindings_to_remove = []
        for binding in bindings:
            binding_found = False
            for server in servicegroup_obj['servers']:
                if binding.get_servername() == server['name']:
                    binding_found = True
                    server['bound'] = True
                    if binding.get_port() != server['port']:
                        binding.set_port(server['port'])
                        try:
                            NSServiceGroupServerBinding.update(nitro, binding)
                        except NSNitroError as error:
                            log.debug('netscaler module error - NSServiceGroupServerBinding.update() failed: {0}'.format(error))
                            ret = False
            if not binding_found:
                binding_to_remove = binding
                try:
                    NSServiceGroupServerBinding.delete(nitro, binding_to_remove)
                except NSNitroError as error:
                    log.debug('netscaler module error - NSServiceGroupServerBinding.remove() failed: {0}'.format(error))
                    ret = False


        for server in servicegroup_obj['servers']:
            if not server.has_key('bound'):
                new_binding = NSServiceGroupServerBinding()
                new_binding.set_servicegroupname(servicegroup_obj['name'])
                new_binding.set_servername(server['name'])
                new_binding.set_port(server['port'])
                try:
                    NSServiceGroupServerBinding.add(nitro, new_binding)
                except NSNitroError as error:
                    log.debug('netscaler module error - NSServiceGroupServerBinding.add() failed: {0}'.format(error))
                    ret = False

    
    return ret

def ensure_servicegroups_state(nitro,servicegroups_obj):
    ret = True
    
    all_servicegroups = NSServiceGroup.get_all(nitro)
    servicegroups_to_remove = []
    for servicegroup in all_servicegroups:
        found_match = False
        for servicegroup_obj in servicegroups_obj:
            if servicegroup_obj['name'] == servicegroup.get_servicegroupname():
                found_match = True
        if not found_match:
            servicegroups_to_remove.append(servicegroup)

    for servicegroup_obj in servicegroups_obj:
        ensure_servicegroup_state(nitro,servicegroup_obj)

    for servicegroup_to_remove in servicegroups_to_remove:
        try:
            NSServiceGroup.delete(nitro, servicegroup_to_remove)
        except NSNitroError as error:
            log.debug('netscaler module error - NSServiceGroupServerBinding.delete() failed: {0}'.format(error))
            ret = False

    
    return ret

def ensure_servers_state(nitro,servers_obj):
    ret = True
    

    for server_obj in servers_obj:
        ensure_server_state(nitro,server_obj)

    all_servers = NSServer.get_all(nitro)
    servers_to_remove = []
    for server in all_servers:
        found_match = False
        for server_obj in servers_obj:
            if server_obj['name'] == server.get_name():
                found_match = True
        if not found_match:
            servers_to_remove.append(server)

    for server_to_remove in servers_to_remove:
        try:
            NSServer.delete(nitro, server_to_remove)
        except NSNitroError as error:
            log.debug('netscaler module error - NSServer.delete() failed: {0}'.format(error))
            ret = False

    
    return ret

def ensure_lbvservers_state(nitro,lbvservers_obj):
    ret = True
    

    for lbvserver_obj in lbvservers_obj:
        ensure_lbvserver_state(nitro,lbvserver_obj)

    all_lbvservers = NSLBVServer.get_all(nitro)
    lbvservers_to_remove = []
    for lbvserver in all_lbvservers:
        found_match = False
        for lbvserver_obj in lbvservers_obj:
            if lbvserver_obj['name'] == lbvserver.get_name():
                found_match = True
        if not found_match:
            lbvservers_to_remove.append(lbvserver)

    for lbvserver_to_remove in lbvservers_to_remove:
        try:
            NSLBVServer.delete(nitro, lbvserver_to_remove)
        except NSNitroError as error:
            log.debug('netscaler module error - NSLBVServer.delete() failed: {0}'.format(error))
            ret = False

    
    return ret

def ensure_lbvserver_state(nitro, lbvserver_obj):
    ret = True
    
    
    all_lbvservers = NSLBVServer.get_all(nitro)
    matches_found = {}
    for lbvserver in all_lbvservers:
      if lbvserver.get_name() == lbvserver_obj['name'] and lbvserver.get_ipv46() == lbvserver_obj['vip_address'] and lbvserver.get_port == lbvserver_obj['port'] and lbvserver.get_servicetype == lbvserver_obj['protocol']:
          matches_found['full_match'] = lbvserver
      elif lbvserver.get_name() == lbvserver_obj['name']:
          matches_found['name'] = lbvserver
      elif lbvserver.get_ipv46() == lbvserver_obj['vip_address']:
          matches_found['vip_address'] = lbvserver

    if not matches_found.has_key('full_match'):

        check_for_port_and_protocol_match = False

        if matches_found.has_key('vip_address') and not matches_found.has_key('name'):
            updated_lbvserver = matches_found['vip_address']
            updated_lbvserver.set_newname(lbvserver_obj['name'])
            try:
                NSLBVServer.rename(nitro, updated_lbvserver)
            except NSNitroError as error:
                log.debug('netscaler module error - NSLBVServer.update() failed: {0}'.format(error))
                ret = False
            lbvserver = NSLBVServer()
            lbvserver.set_name(lbvserver_obj['name'])
            updated_lbvserver = NSLBVServer.get(nitro,lbvserver)
            check_for_port_and_protocol_match = True

        if matches_found.has_key('name') and not matches_found.has_key('vip_address'):
            updated_lbvserver = matches_found['name']
            updated_lbvserver.set_ipv46(lbvserver_obj['vip_address'])
            try:
                NSLBVServer.update(nitro, updated_lbvserver)
            except NSNitroError as error:
                log.debug('netscaler module error - NSLBVServer.update() failed: {0}'.format(error))
                ret = False
            check_for_port_and_protocol_match = True

        if matches_found.has_key('name') and matches_found.has_key('vip_address'):
            update = True
            lbvserver_to_delete = matches_found['vip_address']
            try:
                NSLBVServer.delete(nitro,lbvserver_to_delete)
            except NSNitroError as error:
                log.debug('netscaler module error - NSLBVServer.delete() failed: {0}'.format(error))
                ret = False 
            updated_lbvserver = matches_found['name']
            updated_lbvserver.set_ipv46(lbvserver_obj['vip_address'])
            try:
                NSLBVServer.update(nitro, updated_lbvserver)
            except NSNitroError as error:
                log.debug('netscaler module error - NSLBVServer.update() failed: {0}'.format(error))
                ret = False
            check_for_port_and_protocol_match = True
        if check_for_port_and_protocol_match:
            update = False
            if updated_lbvserver.get_port() != lbvserver_obj['port']:
                update = True
                updated_lbvserver.set_port(lbvserver_obj['port'])
            if updated_lbvserver.get_servicetype() != lbvserver_obj['protocol']:
                update = True
                updated_lbvserver.set_servicetype(lbvserver_obj['protocol'])
            if update:
                try:
                    NSLBVServer.update(nitro, updated_lbvserver)
                except NSNitroError as error:
                    log.debug('netscaler module error - NSLBVServer.update() failed: {0}'.format(error))
                    ret = False

    existing_lbvserver = NSLBVServer()
    existing_lbvserver.set_name(lbvserver_obj['name'])

    try:
        existing_lbvserver = NSLBVServer.get(nitro,existing_lbvserver)
    except NSNitroError as error:
        log.debug('no existing lbvserver found for {}'.format(lbvserver_obj['name']))
        existing_lbvserver = None

    if existing_lbvserver:
        update_needed = False
        delete_needed = False
        if existing_lbvserver.get_servicetype() != lbvserver_obj['protocol'] or existing_lbvserver.get_port() != lbvserver_obj['port']:
            delete_needed = True
        elif existing_lbvserver.get_ipv46() != lbvserver_obj['vip_address']:
            update_needed = True
            existing_lbvserver.set_ipv46(lbvserver_obj['vip_address'])
        if delete_needed:
            try:
                NSLBVServer.delete(nitro, existing_lbvserver)
            except NSNitroError as error:
                log.debug('netscaler module error - NSLBVServer.delete() failed: {0}'.format(error))
                ret = False
            existing_lbvserver = None
        elif update_needed:
            try:
                NSLBVServer.update(nitro, existing_lbvserver)
            except NSNitroError as error:
                log.debug('netscaler module error - NSLBVServer.update() failed: {0}'.format(error))
                ret = False

    if not existing_lbvserver:
        new_lbvserver = NSLBVServer()
        new_lbvserver.set_name(lbvserver_obj['name'])
        new_lbvserver.set_ipv46(lbvserver_obj['vip_address'])
        new_lbvserver.set_port(lbvserver_obj['port'])
        new_lbvserver.set_servicetype(lbvserver_obj['protocol'])
        try:
            NSLBVServer.add(nitro, new_lbvserver)
        except NSNitroError as error:
            log.debug('netscaler module error - NSLBVServer.add() failed: {0}'.format(error))
            ret = False
        current_lbvserver= new_lbvserver

    lbvserver_servicegroup_binding = NSLBVServerServiceGroupBinding()
    lbvserver_servicegroup_binding.set_name(lbvserver_obj['name'])
    try:
        bindings = NSLBVServerServiceGroupBinding.get(nitro, lbvserver_servicegroup_binding)
    except NSNitroError as error:
        log.debug('no existing lbvserver server bindings found for {}'.format(lbvserver_obj['name']))
        bindings = None

    lbvserver_servicegroup_bindings = {}
    for lbvserver_servicegroup_binding in lbvserver_obj['servicegroup_bindings']:
        lbvserver_servicegroup_bindings[lbvserver_servicegroup_binding] = None

    if bindings != None:
        bindings_to_remove = []
        for binding in bindings:
            binding_found = False
            for lbvserver_servicegroup_binding in lbvserver_servicegroup_bindings.keys():
                if binding.get_servicegroupname() == lbvserver_servicegroup_binding:
                    binding_found = True
                    lbvserver_servicegroup_bindings[lbvserver_servicegroup_binding] = True
            if not binding_found:
                binding_to_remove = binding
                try:
                    NSLBVServerServiceGroupBinding.delete(nitro, binding_to_remove)
                except NSNitroError as error:
                    log.debug('netscaler module error - NSLBVServerServiceGroupBinding.remove() failed: {0}'.format(error))
                    ret = False

    for lbvserver_servicegroup_binding in lbvserver_servicegroup_bindings.keys():
        if lbvserver_servicegroup_bindings[lbvserver_servicegroup_binding] == None:
            new_binding = NSLBVServerServiceGroupBinding()
            new_binding.set_name(lbvserver_obj['name'])
            new_binding.set_servicegroupname(lbvserver_servicegroup_binding)
            try:
                NSLBVServerServiceGroupBinding.add(nitro, new_binding)
            except NSNitroError as error:
                log.debug('netscaler module error - NSLBVServerServiceGroupBinding.add() failed: {0}'.format(error))
                ret = False

    
    return ret


def ensure_cs_action_state(nitro, cs_action):
    ret = True
    

    

def get_cs_action(nitro,cs_action_name):
    ret = True
    

    

def main():
    print 'Using config file: {}'.format(sys.argv[1])
    conf = get_config_yaml(sys.argv[1])
    for ns_group in conf['ns_groups']:
        log.info('Processing group {}'.format(ns_group['name']))
        ns_instance = ns_group['ns_instance']
        nitro = connect(ns_instance)
        ensure_servers_state(nitro,ns_group['servers'])
        ensure_servicegroups_state(nitro,ns_group['serviceGroups'])
        ensure_lbvservers_state(nitro,ns_group['lb_vservers'])
        disconnect(nitro)

if __name__ == "__main__": main()