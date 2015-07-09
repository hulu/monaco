#!/usr/bin/python
# codeing=utf-8
'''
This module provides an interface to monaco for exposing Monaco DBs
If this isn't subclassed, DBs can only be accessed directly from Monaco hosts
(cluster allocation information is available from the web API: /api/app/<app_id>)
'''
from schema import MonacoNode

class Expose(object):
    '''
    This should be subclassed to define a site specific exposure method
    (hardware loadbalancing, DNS, haproxy, ...)
    '''

    def app_exposure(self, app, r):
        '''
        Returns a reachable (host, port) pair that the app can be reached at
        '''
        for node_id, role in app.nodes.iteritems():
            if role == 'master':
                node = MonacoNode(node_id=node_id)
                node.refresh(r)
                return (node.FQDN, app.port)
        return None

    def create_app(self, app):
        '''
        Creates an exposure for a schema.App
        '''
        return True

    def delete_app(self, app):
        '''
        Deletes an exposure for a schema.App
        '''
        return True

    def update_app(self, app):
        '''
        Updates (creates if non-existent) an exposure for a schema.App
        '''
        return True

    # These are used to attempt a interruption-less transition of a master
    # (exposure for slave w/ writes is created, exposure for master is deleted, then slave is promoted)
    def create_app_host(self, app, host):
        '''
        Enables/Adds a host to an App exposure
        '''
        return True

    def delete_app_host(self, app, host):
        '''
        Disables/Removes a host from an App exposure
        '''
        return True

    # These are used when slave-exposure is desired
    # By subclassing, you can decide whether to expose all redis-servers or exclusively slaves
    def create_app_slave(self, app):
        '''
        Creates an exposure for a schema.App's slave nodes
        '''
        return True

    def delete_app_slave(self, app):
        '''
        Deletes an exposure for a schema.App's slave nodes
        '''
        return True

    def update_app_slave(self, app):
        '''
        Updates (creates if non-existsent) an exposure for a schema.App's slave nodes
        '''
        return True

    # These are used to expose twemproxies
    # Often these don't need to be implemented as twemproxies provide an exposure for Monaco DBs
    # (and by default, twems are propagated to ALL monaco nodes)
    def create_twem(self, twem):
        '''
        Creates an exposure for a Twemproxy
        '''
        return True

    def delete_twem(self, twem):
        '''
        Deletes an exposure for a Twemproxy
        '''
        return True

    def update_twem(self, twem):
        '''
        Updates (creates if necessary) an exposure for a Twemproxy
        '''
        return True

    # These are used for updating a twemproxy config
    # Since twemproxy has no hot-reload, each proxy host is 'deleted' from exposure
    # while being updated, and then the exposure is 'created' after success.
    def create_twem_host(self, twem, host):
        '''
        Enables/Adds a host to a twem exposure
        '''
        return True

    def delete_twem_host(self, twem, host):
        '''
        Disables/Removes a host from a twem exposure
        '''
        return True
