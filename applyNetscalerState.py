__author__ = 'mike'

import yaml

stram = open("ns.yml","r")
conf = yaml.load(stram)

for cluster in conf['clusters']:
    for nsInstance in cluster['nsInstances']:
      ipAddress = nsInstance['ipAddress']
      userName = nsInstance['user']
      password = nsInstance['pass']
      print 'nsip: {}  user: {}  pass: {}'.format(ipAddress,userName,password)

    for serviceGroup in cluster['serviceGroups']:
        sgName = serviceGroup['name']
        for server in serviceGroup['servers']:
            serverIp = server['ipAddress']
            serverPort = server['port']
            print 'sg: {} ip: {} port: {} protocol: {}'.format(sgName,serverIp,serverPort,serviceGroup['protocol'])

    for lbvserver in cluster['lbvservers']:
        lbvserverName = lbvserver['name']
        lbvserverIpAddress = lbvserver['vipAddress']
        lbvserverServiceGroupBinding = lbvserver['serviceGroupBinding']
        print 'lb: {} ip: {} binding: {}'.format(lbvserverName,lbvserverIpAddress,lbvserverServiceGroupBinding)

    for csvserver in cluster['csvservers']:
        csvserverName = csvserver['name']
        csvserverIpAddress = csvserver['vipAddress']
        csvserverDefaultLbvserver = csvserver['defaultLbvserver']
        print 'cs: {} ip: {} default: {}'.format(csvserverName,csvserverIpAddress,csvserverDefaultLbvserver)
        for binding in csvserver['policyBinding']:
            print 'cs: {} binding: {}'.format(csvserverName,binding)

    for csPolicy in cluster['policies']['cs']:
        csPolicyName = csPolicy['name']
        csPolicyPriority = csPolicy['priority']
        csPolicyExpression = csPolicy['expression']
        csPolicyAction = csPolicy['action']
        print 'policyName: {} priority: {} action: {} expression: {}'.format(csPolicyName,csPolicyPriority, csPolicyAction, csPolicyExpression)

