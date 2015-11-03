__author__ = 'devopsjedi'

import sys
import yaml
import logging
from time import strftime

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
#from nsnitro.nsresources.nssslvserversslcertkeybinding import NSSSLVServerSSLCertKeyBinding


log = logging.getLogger('applyNetscalerState')
log.setLevel(logging.DEBUG)
stream = logging.StreamHandler()
stream.setLevel(logging.DEBUG)
file = logging.FileHandler(LOG_FILENAME)
log.addHandler(stream)
log.addHandler(file)

def get_config_yaml(filename):
    stream = open(filename,"r")
    conf = yaml.load(stream)
    return conf


def connect(ns_instance):
    nitro = NSNitro(ns_instance['address'],ns_instance['user'],ns_instance['pass'])
    try:
        nitro.login()
    except NSNitroError as error:
        log.debug('NSNitro.login() failed: {0}'.format(error))
    return nitro


def disconnect(nitro):
    try:
        nitro.logout()
    except NSNitroError as error:
        log.debug('NSNitro.logout() failed: {0}'.format(error))
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
            server_to_delete = matches_found['ip_address']
            try:
                NSServer.delete(nitro,server_to_delete)
            except NSNitroError as error:
                log.debug('NSServer.delete() failed: {0}'.format(error))
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
            updated_server.set_ipaddress(server_obj['ip_address'])
            try:
                NSServer.update(nitro, updated_server)
            except NSNitroError as error:
                log.debug('NSServer.update() failed: {0}'.format(error))
                ret = False
        if not update:
            new_server = NSServer()
            new_server.set_name(server_obj['name'])
            new_server.set_ipaddress(server_obj['ip_address'])
            try:
                NSServer.add(nitro,new_server)
            except NSNitroError as error:
                log.debug('NSServer.add() failed: {0}'.format(error))
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
                log.debug('NSServiceGroup.delete() failed: {0}'.format(error))
                ret = False
            existing_servicegroup = None

    if not existing_servicegroup:
        new_servicegroup = NSServiceGroup()
        new_servicegroup.set_servicegroupname(servicegroup_obj['name'])
        new_servicegroup.set_servicetype(servicegroup_obj['protocol'])
        try:
            NSServiceGroup.add(nitro, new_servicegroup)
        except NSNitroError as error:
            log.debug('NSServiceGroup.add() failed: {0}'.format(error))
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
                            log.debug('NSServiceGroupServerBinding.update() failed: {0}'.format(error))
                            ret = False
            if not binding_found:
                binding_to_remove = binding
                try:
                    NSServiceGroupServerBinding.delete(nitro, binding_to_remove)
                except NSNitroError as error:
                    log.debug('NSServiceGroupServerBinding.remove() failed: {0}'.format(error))
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
                    log.debug('NSServiceGroupServerBinding.add() failed: {0}'.format(error))
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
            log.debug('NSServiceGroupServerBinding.delete() failed: {0}'.format(error))
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
            log.debug('NSServer.delete() failed: {0}'.format(error))
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
            log.debug('NSLBVServer.delete() failed: {0}'.format(error))
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
                log.debug('NSLBVServer.update() failed: {0}'.format(error))
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
            updated_lbvserver.set_ipv46(lbvserver_obj['vip_address'])
            try:
                NSLBVServer.update(nitro, updated_lbvserver)
            except NSNitroError as error:
                log.debug('NSLBVServer.update() failed: {0}'.format(error))
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
                    log.debug('NSLBVServer.update() failed: {0}'.format(error))
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
        new_lbvserver.set_name(lbvserver_obj['name'])
        new_lbvserver.set_ipv46(lbvserver_obj['vip_address'])
        new_lbvserver.set_port(lbvserver_obj['port'])
        new_lbvserver.set_servicetype(lbvserver_obj['protocol'])
        try:
            NSLBVServer.add(nitro, new_lbvserver)
        except NSNitroError as error:
            log.debug('NSLBVServer.add() failed: {0}'.format(error))
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
                    log.debug('NSLBVServerServiceGroupBinding.remove() failed: {0}'.format(error))
                    ret = False

    for lbvserver_servicegroup_binding in lbvserver_servicegroup_bindings.keys():
        if lbvserver_servicegroup_bindings[lbvserver_servicegroup_binding] == None:
            new_binding = NSLBVServerServiceGroupBinding()
            new_binding.set_name(lbvserver_obj['name'])
            new_binding.set_servicegroupname(lbvserver_servicegroup_binding)
            try:
                NSLBVServerServiceGroupBinding.add(nitro, new_binding)
            except NSNitroError as error:
                log.debug('NSLBVServerServiceGroupBinding.add() failed: {0}'.format(error))
                ret = False

    
    return ret


def ensure_cs_action_state(nitro, cs_action):
    ret = True
    
def add_cs_action(nitro,cs_action_obj):
    ret = True
    cs_action = NSBaseResource()
    cs_action.set_options({'name':cs_action_obj['name'], 'targetlbvserver':cs_action_obj['target_lbvserver']})
    cs_action.resourcetype = 'csaction'
    try:
        cs_action.add_resource(nitro)
    except NSNitroError as error:
        log.debug('NSBaseResource.add_resource() failed: {0}'.format(error))
        ret = False
    return ret
    

def get_cs_action(nitro,cs_action_name):
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

def ensure_cs_action_state(nitro,cs_action_obj):
    ret = True
    existing_cs_action = get_cs_action(nitro,cs_action_obj['name'])

    if existing_cs_action:
        if existing_cs_action.options['targetlbvserver'] != cs_action_obj['target_lbvserver']:
            existing_cs_action.options['targetlbvserver'] = cs_action_obj['target_lbvserver']
            update_cs_action(nitro, existing_cs_action)
    else:
        ret = add_cs_action(nitro, cs_action_obj)
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


def ensure_cs_actions_state(nitro, cs_actions_obj):
    ret = True

    all_cs_actions = get_all_cs_actions(nitro)
    cs_actions_to_delete = []

    if all_cs_actions != None:
        for existing_cs_action in all_cs_actions:
            found_match = False
            for cs_action_obj in cs_actions_obj:
                if existing_cs_action.options['name'] == cs_action_obj['name']:
                    found_match = True
                    ensure_cs_action_state(nitro,cs_action_obj)
                    cs_action_obj['existing'] = True

            if not found_match:
                delete_cs_action(nitro,existing_cs_action.options['name'])


    for cs_action_obj in cs_actions_obj:
        if not cs_action_obj.has_key('existing'):
            ensure_cs_action_state(nitro,cs_action_obj)

    return ret


def ensure_cs_policies_state(nitro,cs_policies_obj):
    ret = True
    try:
        all_cs_policies = NSCSPolicy().get_all(nitro)
    except NSNitroError as error:
        pass

    len(all_cs_policies)
    for cs_policy_obj in cs_policies_obj:
        found_match = False
        if all_cs_policies:
            for cs_policy in all_cs_policies:
                if cs_policy.get_policyname() == cs_policy_obj['name']:
                    found_match = True
                    if not ensure_cs_policy_state(nitro,cs_policy_obj):
                        ret = False
                    cs_policy_obj['existing'] = True

                if not found_match:
                    delete_cs_policy(nitro,cs_policy)

    for cs_policy_obj in cs_policies_obj:
        if not cs_policy_obj.has_key('existing'):
           if not ensure_cs_policy_state(nitro,cs_policy_obj):
               ret = False

    return ret

def delete_cs_policy(nitro,cs_policy):
    ret = True
    url = "%s%s/%s" % (nitro.get_url(), cs_policy.resourcetype, cs_policy.get_policyname())
    try:
        response = nitro.delete(url)
    except NSNitroError as error:
        log.debug('delete_cs_policy failed: {0}'.format(error))
        ret = False
    return ret




def ensure_cs_policy_state(nitro,cs_policy_obj):
    ret = True

    try:
        url = nitro.get_url() + 'cspolicy/' + cs_policy_obj['name']
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
            if existing_cs_policy.get_rule() != cs_policy_obj['expression']:
                update = True
                existing_cs_policy.set_rule(cs_policy_obj['expression'])
            if existing_cs_policy.options.has_key('action'):
                if existing_cs_policy.options['action'] != cs_policy_obj['action']:
                    update = True
                    existing_cs_policy.options['action'] = cs_policy_obj['action']
            else:
                update = True
                existing_cs_policy.options['action'] = cs_policy_obj['action']
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
        new_cs_policy = NSCSPolicy({'policyname':cs_policy_obj['name'],'rule':cs_policy_obj['expression']})
        new_cs_policy.options['action'] = cs_policy_obj['action']
        try:
            new_cs_policy.add_resource(nitro)
        except NSNitroError as error:
            log.debug('NSCSPolicy.add() failed: {0}'.format(error))


def ensure_csvservers_state(nitro,csvservers_obj):
    ret = True
    

    for csvserver_obj in csvservers_obj:
        ensure_csvserver_state(nitro,csvserver_obj)

    all_csvservers = NSCSVServer.get_all(nitro)
    csvservers_to_remove = []
    for csvserver in all_csvservers:
        found_match = False
        for csvserver_obj in csvservers_obj:
            if csvserver_obj['name'] == csvserver.get_name():
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

def add_csvserver_lbvserver_binding(nitro, csvserver_obj):
    ret = False
    new_binding = NSBaseResource()
    new_binding.resourcetype = 'csvserver_lbvserver_binding'
    new_binding.options['name'] = csvserver_obj['name']
    new_binding.options['lbvserver'] = csvserver_obj['default_lbvserver']
    try:
        new_binding.add_resource(nitro)
        ret = True
    except NSNitroError as error:
        log.debug('add_csvserver_lbvserver_binding() failed: {0}'.format(error))
    return ret


def ensure_csvserver_state(nitro, csvserver_obj):
    ret = True
    
    
    all_csvservers = NSCSVServer.get_all(nitro)
    matches_found = {}
    for csvserver in all_csvservers:
      if csvserver.get_name() == csvserver_obj['name'] and csvserver.get_ipv46() == csvserver_obj['vip_address'] and csvserver.get_port == csvserver_obj['port'] and csvserver.get_servicetype == csvserver_obj['protocol']:
          matches_found['full_match'] = csvserver
      elif csvserver.get_name() == csvserver_obj['name']:
          matches_found['name'] = csvserver
      elif csvserver.get_ipv46() == csvserver_obj['vip_address']:
          matches_found['vip_address'] = csvserver

    if not matches_found.has_key('full_match'):

        check_for_port_and_protocol_match = False

        if matches_found.has_key('vip_address') and not matches_found.has_key('name'):
            updated_csvserver = matches_found['vip_address']
            updated_csvserver.set_newname(csvserver_obj['name'])
            try:
                NSCSVServer.rename(nitro, updated_csvserver)
            except NSNitroError as error:
                log.debug('NSCSVServer.update() failed: {0}'.format(error))
                ret = False
            csvserver = NSCSVServer()
            csvserver.set_name(csvserver_obj['name'])
            updated_csvserver = NSCSVServer.get(nitro,csvserver)
            check_for_port_and_protocol_match = True

        if matches_found.has_key('name') and not matches_found.has_key('vip_address'):
            updated_csvserver = matches_found['name']
            updated_csvserver.set_ipv46(csvserver_obj['vip_address'])
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
            updated_csvserver.set_ipv46(csvserver_obj['vip_address'])
            try:
                NSCSVServer.update(nitro, updated_csvserver)
            except NSNitroError as error:
                log.debug('NSCSVServer.update() failed: {0}'.format(error))
                ret = False
            check_for_port_and_protocol_match = True
        if check_for_port_and_protocol_match:
            update = False
            if updated_csvserver.get_port() != csvserver_obj['port']:
                update = True
                updated_csvserver.set_port(csvserver_obj['port'])
            if updated_csvserver.get_servicetype() != csvserver_obj['protocol']:
                update = True
                updated_csvserver.set_servicetype(csvserver_obj['protocol'])
            if update:
                try:
                    NSCSVServer.update(nitro, updated_csvserver)
                except NSNitroError as error:
                    log.debug('NSCSVServer.update() failed: {0}'.format(error))
                    ret = False

    existing_csvserver = NSCSVServer()
    existing_csvserver.set_name(csvserver_obj['name'])

    try:
        existing_csvserver = NSCSVServer.get(nitro,existing_csvserver)
    except NSNitroError as error:
        log.debug('no existing csvserver found for {}'.format(csvserver_obj['name']))
        existing_csvserver = None

    if existing_csvserver:
        update_needed = False
        delete_needed = False
        if existing_csvserver.get_servicetype() != csvserver_obj['protocol'] or existing_csvserver.get_port() != csvserver_obj['port']:
            delete_needed = True
        elif existing_csvserver.get_ipv46() != csvserver_obj['vip_address']:
            update_needed = True
            existing_csvserver.set_ipv46(csvserver_obj['vip_address'])
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
        new_csvserver.set_name(csvserver_obj['name'])
        new_csvserver.set_ipv46(csvserver_obj['vip_address'])
        new_csvserver.set_port(csvserver_obj['port'])
        new_csvserver.set_servicetype(csvserver_obj['protocol'])
        try:
            NSCSVServer.add(nitro, new_csvserver)
        except NSNitroError as error:
            log.debug('NSCSVServer.add() failed: {0}'.format(error))
            ret = False
        current_csvserver= new_csvserver

    existing_csvserver_lbvserver_binding = get_csvserver_lbvserver_binding(nitro,csvserver_obj['name'])
    if csvserver_obj.has_key('default_lbvserver'):
        if existing_csvserver_lbvserver_binding != None:
            if existing_csvserver_lbvserver_binding.options['lbvserver'] != csvserver_obj['default_lbvserver']:
                existing_csvserver_lbvserver_binding.options['lbvserver'] = csvserver_obj['default_lbvserver']
                update_csvserver_lbvserver_binding(nitro,existing_csvserver_lbvserver_binding)
        else:
            add_csvserver_lbvserver_binding(nitro,csvserver_obj)
    elif existing_csvserver_lbvserver_binding != None:
        delete_csvserver_lbvserver_binding(nitro,csvserver_obj['name'])


    csvserver_policy_binding = NSCSVServerCSPolicyBinding()
    csvserver_policy_binding.set_name(csvserver_obj['name'])
    try:
        existing_bindings = NSCSVServerCSPolicyBinding.get(nitro, csvserver_policy_binding)
    except NSNitroError as error:
        log.debug('no existing csvserver policy bindings found for {}'.format(csvserver_obj['name']))
        existing_bindings = None

    for binding in csvserver_obj['policy_bindings']:
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

        for binding in csvserver_obj['policy_bindings']:
            if not binding.has_key('existing'):
                new_binding = NSCSVServerCSPolicyBinding()
                new_binding.set_name(csvserver_obj['name'])
                new_binding.set_policyname(binding['name'])
                new_binding.set_priority(binding['priority'])
                try:
                    NSCSVServerCSPolicyBinding.add(nitro, new_binding)
                except NSNitroError as error:
                    log.debug('NSCSVServerCSPolicyBinding.add() failed: {0}'.format(error))
                    ret = False

    
    return ret


def main():
    print 'Using config file: {}'.format(sys.argv[1])
    conf = get_config_yaml(sys.argv[1])
    for ns_group in conf['ns_groups']:
        log.info('Processing group {}'.format(ns_group['name']))
        ns_instance = ns_group['ns_instance']
        nitro = connect(ns_instance)
        ensure_servers_state(nitro,ns_group['servers'])
        ensure_servicegroups_state(nitro,ns_group['serviceGroups'])
        ensure_lbvservers_state(nitro,ns_group['lbvservers'])
        ensure_cs_actions_state(nitro,ns_group['cs_actions'])
        ensure_cs_policies_state(nitro,ns_group['cs_policies'])
        ensure_csvservers_state(nitro,ns_group['csvservers'])
        disconnect(nitro)

if __name__ == "__main__": main()