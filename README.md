# netscaler-yml-config

# Overview
This script processes a YAML file that contains a Netscaler state and applies it to one or more appliances.

# Supported States
## Servers
Server definition for Load Balancing
* Name
* IP Address

## Service Groups
Service groups for Load Balancing
* Name
* Protocol
* Server bindings to Service Groups
  * Server Name
  * Port

## Load Balancing
Virtual server for Load Balancing
* Name
* Virtual IP Address
* Port
* Protocol
* Service Group Bindings

# Input
The script accepts the filename of a YAML-based configuration ("ns.yml" for example)


