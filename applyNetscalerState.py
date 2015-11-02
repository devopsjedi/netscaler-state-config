__author__ = 'mike'

import sys
import yaml
import json
import logging

LOG_FILENAME = 'applyNetscalerState.log'

from nsnitro.nsnitro import NSNitro
from nsnitro.nsexceptions import NSNitroError
from nsnitro.nsresources.nsserver import NSServer
from nsnitro.nsresources.nsservice import NSService
from nsnitro.nsresources.nsservicegroup import NSServiceGroup
from nsnitro.nsresources.nsservicegroupserverbinding import NSServiceGroupServerBinding
from nsnitro.nsresources.nslbvserver import NSLBVServer
from nsnitro.nsresources.nslbvserverservicegroupbinding import NSLBVServerServiceGroupBinding
from nsnitro.nsresources.nssslvserversslcertkeybinding import NSSSLVServerSSLCertKeyBinding


log = logging.getLogger('applyNetscalerState')
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
log.addHandler(ch)

def get_config_yaml(filename):
    stream = open(filename,"r")
    conf = yaml.load(stream)
    return conf


def connect(nsInstance):
    nitro = NSNitro(nsInstance['ip_address'],nsInstance['user'],nsInstance['pass'])
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


def servicegroup_get(sg_name):
    nitro = connect(nsInstance)
    if nitro is None:
        return None
    sg = NSServiceGroup()
    sg.set_servicegroupname(sg_name)
    try:
        sg = NSServiceGroup.get(nitro, sg)
    except NSNitroError as error:
        log.debug('netscaler module error - NSServiceGroup.get() failed: {0}'.format(error))
        sg = None
    disconnect(nitro)
    return sg


def servicegroup_get_servers(sg_name):
    nitro = connect(nsInstance)
    if nitro is None:
        return None
    sg = NSServiceGroup()
    sg.set_servicegroupname(sg_name)
    try:
        sg = NSServiceGroup.get_servers(nitro, sg)
    except NSNitroError as error:
        log.debug('netscaler module error - NSServiceGroup.get_servers failed(): {0}'.format(error))
        sg = None
    disconnect(nitro)
    return sg


def servicegroup_get_server(sg_name, s_name, s_port=None ):
    ret = None
    servers = servicegroup_get_servers(sg_name)
    if servers is None:
        return None
    for server in servers:
        if server.get_servername() == s_name:
            if s_port is not None and s_port != server.get_port():
                ret = None
            ret = server
    return ret


def servicegroup_exists(sg_name, sg_type=None ):
    sg = servicegroup_get(sg_name)
    if sg is None:
        return False
    if sg_type is not None and sg_type.upper() != sg.get_servicetype():
        return False
    return True


def servicegroup_add(sg_name, sg_type='HTTP'):
    ret = True
    if servicegroup_exists(sg_name):
        return False
    nitro = connect(nsInstance)
    if nitro is None:
        return False
    sg = NSServiceGroup()
    sg.set_servicegroupname(sg_name)
    sg.set_servicetype(sg_type.upper())
    try:
        NSServiceGroup.add(nitro, sg)
    except NSNitroError as error:
        log.debug('netscaler module error - NSServiceGroup.add() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def servicegroup_delete(sg_name):
    ret = True
    sg = servicegroup_get(sg_name)
    if sg is None:
        return False
    nitro = connect(nsInstance)
    if nitro is None:
        return False
    try:
        NSServiceGroup.delete(nitro, sg)
    except NSNitroError as error:
        log.debug('netscaler module error - NSServiceGroup.delete() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def servicegroup_server_exists(sg_name, s_name, s_port=None):
    return servicegroup_get_server(sg_name, s_name, s_port) is not None


def servicegroup_server_up(sg_name, s_name, s_port):
    server = servicegroup_get_server(sg_name, s_name, s_port)
    #log.debug('state of {0}:{1} is {2}'.format(server.get_servername(), server.get_port(), server.get_svrstate()))
    return server is not None and server.get_svrstate() == 'UP'


def servicegroup_server_enable(sg_name, s_name, s_port):

    ret = True
    server = servicegroup_get_server(sg_name, s_name, s_port)
    if server is None:
        return False
    nitro = connect(nsInstance)
    if nitro is None:
        return False
    try:
        NSServiceGroup.enable_server(nitro, server)
    except NSNitroError as error:
        log.debug('netscaler module error - NSServiceGroup.enable_server() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def servicegroup_server_disable(sg_name, s_name, s_port):

    ret = True
    server = servicegroup_get_server(sg_name, s_name, s_port)
    if server is None:
        return False
    nitro = connect(nsInstance)
    if nitro is None:
        return False
    try:
        NSServiceGroup.disable_server(nitro, server)
    except NSNitroError as error:
        log.debug('netscaler module error - NSServiceGroup.disable_server() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def servicegroup_server_add(sg_name, s_name, s_port):

    ret = True
    server = servicegroup_get_server(sg_name, s_name, s_port)
    if server is not None:
        return False
    nitro = connect(nsInstance)
    if nitro is None:
        return False
    sgsb = NSServiceGroupServerBinding()
    sgsb.set_servicegroupname(sg_name)
    sgsb.set_servername(s_name)
    sgsb.set_port(s_port)
    try:
        NSServiceGroupServerBinding.add(nitro, sgsb)
    except NSNitroError as error:
        log.debug('netscaler module error - NSServiceGroupServerBinding() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def servicegroup_server_delete(sg_name, s_name, s_port):
    ret = True
    server = servicegroup_get_server(sg_name, s_name, s_port)
    if server is None:
        return False
    nitro = connect(nsInstance)
    if nitro is None:
        return False
    sgsb = NSServiceGroupServerBinding()
    sgsb.set_servicegroupname(sg_name)
    sgsb.set_servername(s_name)
    sgsb.set_port(s_port)
    try:
        NSServiceGroupServerBinding.delete(nitro, sgsb)
    except NSNitroError as error:
        log.debug('netscaler module error - NSServiceGroupServerBinding() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def service_get(s_name, nsInstance):
    nitro = connect(nsInstance)
    if nitro is None:
        return None
    service = NSService()
    service.set_name(s_name)
    try:
        service = NSService.get(nitro, service)
    except NSNitroError as error:
        log.debug('netscaler module error - NSService.get() failed: {0}'.format(error))
        service = None
    disconnect(nitro)
    return service


def service_exists(s_name, nsInstance):
    return service_get(s_name, nsInstance) is not None

def service_add(serviceObj, nsInstance):
    serviceJson = json.dumps(serviceObj)
    service = NSService(serviceJson)
    ret = True
    if service is None:
        return False
    nitro = connect(nsInstance)
    if nitro is None:
        return False
    try:
        NSService.add(nitro, service)
    except NSNitroError as error:
        log.debug('netscaler module error - NSService.enable() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def service_up(s_name):
    service = service_get(s_name)
    return service is not None and service.get_svrstate() == 'UP'


def service_enable(s_name):
    ret = True
    service = service_get(s_name)
    if service is None:
        return False
    nitro = connect(nsInstance)
    if nitro is None:
        return False
    try:
        NSService.enable(nitro, service)
    except NSNitroError as error:
        log.debug('netscaler module error - NSService.enable() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def service_disable(s_name, s_delay=None):
    ret = True
    service = service_get(s_name)
    if service is None:
        return False
    if s_delay is not None:
        service.set_delay(s_delay)
    nitro = connect(nsInstance)
    if nitro is None:
        return False
    try:
        NSService.disable(nitro, service)
    except NSNitroError as error:
        log.debug('netscaler module error - NSService.enable() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def vserver_get(v_name):
    nitro = connect(nsInstance)
    vserver = NSLBVServer()
    vserver.set_name(v_name)
    if nitro is None:
        return None
    try:
        vserver = NSLBVServer.get(nitro, vserver)
    except NSNitroError as error:
        log.debug('netscaler module error - NSLBVServer.get() failed: {0}'.format(error))
        vserver = None
    disconnect(nitro)
    return vserver


def vserver_exists(v_name, v_ip=None, v_port=None, v_type=None):
    vserver = vserver_get(v_name)
    if vserver is None:
        return False
    if v_ip is not None and vserver.get_ipv46() != v_ip:
        return False
    if v_port is not None and vserver.get_port() != v_port:
        return False
    if v_type is not None and vserver.get_servicetype().upper() != v_type.upper():
        return False
    return True


def vserver_add(v_name, v_ip, v_port, v_type):
    ret = True
    if vserver_exists(v_name):
        return False
    nitro = connect(nsInstance)
    if nitro is None:
        return False
    vserver = NSLBVServer()
    vserver.set_name(v_name)
    vserver.set_ipv46(v_ip)
    vserver.set_port(v_port)
    vserver.set_servicetype(v_type.upper())
    try:
        NSLBVServer.add(nitro, vserver)
    except NSNitroError as error:
        log.debug('netscaler module error - NSLBVServer.add() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def vserver_delete(v_name):
    ret = True
    vserver = vserver_get(v_name)
    if vserver is None:
        return False
    nitro = connect(nsInstance)
    if nitro is None:
        return False
    try:
        NSLBVServer.delete(nitro, vserver)
    except NSNitroError as error:
        log.debug('netscaler module error - NSVServer.delete() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def vserver_servicegroup_get(v_name, sg_name):
    ret = None
    nitro = connect(nsInstance)
    if nitro is None:
        return None
    vsg = NSLBVServerServiceGroupBinding()
    vsg.set_name(v_name)
    try:
        vsgs = NSLBVServerServiceGroupBinding.get(nitro, vsg)
    except NSNitroError as error:
        log.debug('netscaler module error - NSLBVServerServiceGroupBinding.get() failed: {0}'.format(error))
        return None
    for vsg in vsgs:
        if vsg.get_servicegroupname() == sg_name:
            ret = vsg
    disconnect(nitro)
    return ret


def vserver_servicegroup_exists(v_name, sg_name):
    return vserver_servicegroup_get(v_name, sg_name) is not None


def vserver_servicegroup_add(v_name, sg_name):
    ret = True
    if vserver_servicegroup_exists(v_name, sg_name):
        return False
    nitro = connect(nsInstance)
    if nitro is None:
        return False
    vsg = NSLBVServerServiceGroupBinding()
    vsg.set_name(v_name)
    vsg.set_servicegroupname(sg_name)
    try:
        NSLBVServerServiceGroupBinding.add(nitro, vsg)
    except NSNitroError as error:
        log.debug('netscaler module error - NSLBVServerServiceGroupBinding.add() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def vserver_servicegroup_delete(nsInstance, v_name, sg_name):
    ret = True
    if not vserver_servicegroup_exists(v_name, sg_name):
        return False
    nitro = connect(nsInstance)
    if nitro is None:
        return False
    vsg = NSLBVServerServiceGroupBinding()
    vsg.set_name(v_name)
    vsg.set_servicegroupname(sg_name)
    try:
        NSLBVServerServiceGroupBinding.delete(nitro, vsg)
    except NSNitroError as error:
        log.debug('netscaler module error - NSLBVServerServiceGroupBinding.delete() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def server_get(nsInstance, s_name):
    nitro = connect(nsInstance)
    if nitro is None:
        return None
    server = NSServer()
    server.set_name(s_name)
    try:
        server = NSServer.get(nitro, server)
    except NSNitroError as error:
        log.debug('netscaler module error - NSServer.get() failed: {0}'.format(error))
        server = None
    disconnect(nitro)
    return server


def ensure_server_state(nsInstance, server_obj):
    ret = True
    nitro = connect(nsInstance)
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


def ensure_servicegroup_state(nsInstance, servicegroup_obj):
    ret = True
    nitro = connect(nsInstance)
    existing_servicegroup = NSServiceGroup()
    existing_servicegroup.set_servicegroupname(servicegroup_obj['name'])
    try:
        existing_servicegroup = NSServiceGroup.get(nitro,existing_servicegroup)
    except NSNitroError as error:
        log.debug('no existing servicegroup found for {}'.format(servicegroup_obj['name']))
        existing_servicegroup = None

    if existing_servicegroup != None:
        if existing_servicegroup.get_servicetype() != servicegroup_obj['protocol']:
            existing_servicegroup.set_servicetype(servicegroup_obj['protocol'])
            try:
                NSServiceGroup.update(nitro, existing_servicegroup)
            except NSNitroError as error:
                log.debug('netscaler module error - NSServiceGroup.update() failed: {0}'.format(error))
                ret = False
            current_servicegroup = existing_servicegroup
    else:
        new_servicegroup = NSServiceGroup()
        new_servicegroup.set_servicegroupname(servicegroup_obj['name'])
        new_servicegroup.set_servicetype(servicegroup_obj['protocol'])
        try:
            NSServiceGroup.add(nitro, new_servicegroup)
        except NSNitroError as error:
            log.debug('netscaler module error - NSServiceGroup.add() failed: {0}'.format(error))
            ret = False
        current_servicegroup = new_servicegroup
    servicegroup_binding = NSServiceGroupServerBinding()
    servicegroup_binding.set_servicegroupname(servicegroup_obj['name'])
    try:
        bindings = NSServiceGroupServerBinding.get(nitro, servicegroup_binding)
    except NSNitroError as error:
        log.debug('no existing servicegroup server bindings found for {}'.format(servicegroup_obj['name']))
        bindings = None

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


def server_add(nsInstance, s_name, s_ip, s_state=None):
    ret = True
    nitro = connect(nsInstance)
    if nitro is None:
        return False
    server = NSServer()
    server.set_name(s_name)
    server.set_ip_address(s_ip)
    if s_state is not None:
        server.set_state(s_state)
    try:
        NSServer.add(nitro, server)
    except NSNitroError as error:
        log.debug('netscaler module error - NSServer.add() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def server_delete(s_name, nsInstance):
    ret = True
    server = server_get(s_name)
    if server is None:
        return False
    nitro =connect(nsInstance)
    if nitro is None:
        return False
    try:
        NSServer.delete(nitro, server)
    except NSNitroError as error:
        log.debug('netscaler module error - NSServer.delete() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def server_update(nsInstance, s_name, s_ip):
    altered = False
    cur_server = server_get(s_name)
    if cur_server is None:
        return False
    alt_server = NSServer()
    alt_server.set_name(s_name)
    if cur_server.get_ip_address() != s_ip:
        alt_server.set_ip_address(s_ip)
        altered = True
    # Nothing to update, the server is already idem
    if altered is False:
        return False
    # Perform the update
    nitro =connect(nsInstance)
    if nitro is None:
        return False
    ret = True
    try:
        NSServer.update(nitro, alt_server)
    except NSNitroError as error:
        log.debug('netscaler module error - NSServer.update() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def server_enabled(s_name, nsInstance):
    server = server_get(s_name)
    return server is not None and server.get_state() == 'ENABLED'


def server_enable(s_name, nsInstance):
    ret = True
    server = server_get(s_name)
    if server is None:
        return False
    if server.get_state() == 'ENABLED':
        return True
    nitro =connect(nsInstance)
    if nitro is None:
        return False
    try:
        NSServer.enable(nitro, server)
    except NSNitroError as error:
        log.debug('netscaler module error - NSServer.enable() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret


def server_disable(s_name, nsInstance):
    ret = True
    server = server_get(s_name)
    if server is None:
        return False
    if server.get_state() == 'DISABLED':
        return True
    nitro =connect(nsInstance)
    if nitro is None:
        return False
    try:
        NSServer.disable(nitro, server)
    except NSNitroError as error:
        log.debug('netscaler module error - NSServer.disable() failed: {0}'.format(error))
        ret = False
    disconnect(nitro)
    return ret

def getServerFromName(serversList, serverName):
    ret = None
    for serverEntry in serversList:
        if serverEntry['name'] == serverName:
            ret = serverEntry
    return ret


def main():
    print sys.argv[1]
    conf = get_config_yaml(sys.argv[1])
    if True:
        for cluster in conf['clusters']:
            for nsInstance in cluster['nsInstances']:
              ip_address = nsInstance['ip_address']
              userName = nsInstance['user']
              password = nsInstance['pass']
              print 'nsip: {}  user: {}  pass: {}'.format(ip_address,userName,password)
              connect(nsInstance)

            for server in cluster['servers']:
                for nsInstance in cluster['nsInstances']:
                    ensure_server_state(nsInstance,server)

            for serviceGroup in cluster['serviceGroups']:
                for nsInstance in cluster['nsInstances']:
                    ensure_servicegroup_state(nsInstance,serviceGroup)



            for lbvserver in cluster['lbvservers']:
                lbvserverName = lbvserver['name']
                lbvserverip_address = lbvserver['vip_address']
                lbvserverServiceGroupBinding = lbvserver['serviceGroupBinding']
                print 'lb: {} ip: {} binding: {}'.format(lbvserverName,lbvserverip_address,lbvserverServiceGroupBinding)

            for csvserver in cluster['csvservers']:
                csvserverName = csvserver['name']
                csvserverip_address = csvserver['vip_address']
                csvserverDefaultLbvserver = csvserver['defaultLbvserver']
                print 'cs: {} ip: {} default: {}'.format(csvserverName,csvserverip_address,csvserverDefaultLbvserver)
                for binding in csvserver['policyBinding']:
                    print 'cs: {} binding: {}'.format(csvserverName,binding)

            for csPolicy in cluster['policies']['cs']:
                csPolicyName = csPolicy['name']
                csPolicyPriority = csPolicy['priority']
                csPolicyExpression = csPolicy['expression']
                csPolicyAction = csPolicy['action']
                print 'policyName: {} priority: {} action: {} expression: {}'.format(csPolicyName,csPolicyPriority, csPolicyAction, csPolicyExpression)

if __name__ == "__main__": main()