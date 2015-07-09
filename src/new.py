#!/usr/bin/python
'''
This script is used to start a new instance of Monaco
'''
USAGE = '''
Usage:

There are two modes:
    - ./usr/monaco/new.py
        - This is for creating a new cluster
    - ./usr/monaco/new.py <member_host> <member_port>
        - This is for joining an existing cluster
        - The host:port pair may be any member of the cluster, optimally the master
        - member_host:member_port will be used to syncronize the mgmt DB
'''
import sys
import subprocess
from redis import StrictRedis

import config as config
from schema import Monaco, MonacoNode, MONACO_NEXT_APP_ID, MONACO_NEXT_TWEM_ID, MIN_APP_ID, MIN_TWEM_ID
from redismgmt import RedisMgmt


if __name__ == '__main__':
    rmgr = RedisMgmt()
    if len(sys.argv) == 1:
        # New cluster case

        # (Re)start the mgmt db, set as master, initialize this node as node_id=1, and launch monaco
        rmgr.restart_instance(app_id='mgmt')
        r = StrictRedis(port=config.config['mgmt_port'])
        r.slaveof()
        r.set(MONACO_NEXT_APP_ID, MIN_APP_ID)
        r.set(MONACO_NEXT_TWEM_ID, MIN_TWEM_ID)
        monaco = Monaco()
        node = MonacoNode(node_id='1')
        node.refresh(r) # not expecting anything
        node.hostname = config.config['hostname']
        node.FQDN = config.config['IP']
        node.write(r)
        monaco.new_node(node, r)

        proc = subprocess.Popen(
            ['start', 'monaco'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        (out, err) = proc.communicate()
        if proc.wait() != 0:
            print out, err
            sys.exit(1)

        print 'The node details should be edited from the Web Interface to set memory capacity before use'
    
    elif len(sys.argv) == 3:
        # New member case

        # (Re)start mgmt db, sync to member, and on completion launch monaco
        rmgr.restart_instance(app_id='mgmt')
        r = StrictRedis(port=config.config['mgmt_port'])
        r.slaveof(sys.argv[1], sys.argv[2])

        resp = raw_input('Have you set up this node via the Web interface? (Y/n)')
        if resp == 'n' or resp == 'N':
            print 'Please add the node via the admin view before launching'
            sys.exit(0)
 
        proc = subprocess.Popen(
            ['start', 'monaco'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        (out, err) = proc.communicate()
        if proc.wait() != 0:
            print out, err
            sys.exit(1)

        print 'Monaco will shortly recognize this node if properly configured!'       
    else:
        print USAGE
        sys.exit(1)
