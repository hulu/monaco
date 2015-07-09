#!/usr/bin/python
from servers.server import ZeroMQPeer, ZeroMQClient, ZeroMQServer
from states.follower import Follower

import sys
import os
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)
from config import config

class LifeRaft(object):
    def __init__(self):
        self.peers = []
        for IP, port in config['raft']['cluster']:
            srv = ZeroMQPeer('%s:%s' % (IP, port), host=IP, port=port)
            self.peers.append(srv)
        if config['raft']['actor']:
            self.server = ZeroMQServer('%s:%s' % (config['IP'], config['raft']['port']), Follower(), [], self.peers, host=config['IP'], port=config['raft']['port'])
            self.server.start()

        self.client = ZeroMQClient(self.peers)

    @property
    def value(self):
        return self.client.leader.split(':')
