#!/usr/bin/python
'''
Defines a stat publishing base Class
'''

class Publish(object):
    ''' Stat publisher inferface '''
    def publish_monaco_stat(self, node_id, stat, value):
        '''
        Publishes a stat for the monaco mgmt db
        '''
        pass

    def publish_app_stat(self, app_id, stat, value):
        '''
        Publishes a stat for the specified app_id
        '''
        pass

    def publish_twem_stat(self, twem_id, node_id, stat, value):
        '''
        Publishes a stat for the specified twem_id/node_id
        '''
        pass
