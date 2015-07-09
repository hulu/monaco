#!/usr/bin/python
'''
Subclass of statpublish.Publish which publishes stats to hulu graphite
'''
import config
import time
import logging
import socket

import statpublish

STATLOGGER = logging.getLogger('monaco.stats')

class Publish(statpublish.Publish):
    ''' Graphite stat publisher '''
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Monaco stat handler
    def publish_monaco_stat(self, node, stat, value):
        '''
        Push stat to graphite
        '''
        try:
            stat_name = 'monaco.%s.%s.6379.%s.%s' % (
                config.config['env'],
                config.config['location'],
                node,
                stat,
            )

            self.sock.sendto(
                '%s %s %s\n' % (stat_name, value, int(time.time())),
                (config.config['stats']['graphite_relay'], 2013),
            )
        except Exception, exc:
            STATLOGGER.warn('Exception publiching monaco stat (%s. %s, %s):\n%s', stat_name, stat, value, repr(exc))

    # App Handler
    def publish_app_stat(self, app, stat, value):
        '''
        Push stat to graphite
        '''
        try:
            stat_name = 'monaco.%s.%s.%s.%s' % (
                config.config['env'],
                config.config['location'],
                app,
                stat,
            )

            self.sock.sendto(
                '%s %s %s\n' % (stat_name, value, int(time.time())),
                (config.config['stats']['graphite_relay'], 2013)
            )
        except Exception, exc:
            STATLOGGER.warn('Exception publishing app stat (%s, %s, %s):\n%s', stat_name, stat, value, repr(exc))


    # Twem Handler
    def publish_twem_stat(self, twem_id, host, stat, value):
        '''
        Push stat to graphite
        '''
        try:
            stat_name = 'monaco.%s.%s.proxy.%s.%s.%s' % (
                config.config['env'],
                config.config['location'],
                twem_id,
                host,
                stat,
            )

            self.sock.sendto(
                '%s %s %s\n' % (stat_name, value, int(time.time())),
                (config.config['stats']['graphite_relay'], 2013)
            )
        except Exception, exc:
            STATLOGGER.warn('Exception publishing twem stat (%s, %s, %s):\n%s', twem_id, stat, value, repr(exc))
