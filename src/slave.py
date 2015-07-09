#!/usr/bin/python
# coding=utf-8
'''
Provides the aspect of monaco that manages user-dbs
Using pub/sub on keyevent channels, the slave propagates changes as
they're made in the mgmt db.
'''
from __future__ import print_function, division

import threading
import redis
import logging
# For slave-based health checks
import atexit
from apscheduler.scheduler import Scheduler
from multiprocessing.pool import ThreadPool

from config import config
import schema
from schema import keysub
from redismgmt import RedisMgmt
from nutmgmt import NutMgmt


class Slave(object):
    '''
    Uses redis pub sub to coordinate updates to this instance's apps
    This runs on the master too, and coordinates the nodes activities

    Does this by subscribing to key events for both this node's app set
    as well as all the assigned apps' cluster hashes.
    '''
    def __init__(self):
        monaco = schema.Monaco()
        self.r = redis.StrictRedis(port=config['mgmt_port'])
        # terniaries are always a bad idea. this is a mess of exceptions waiting to cascade so FIXME
        if self.r.info()['role'] == 'master':
            self.rmaster = redis.StrictRedis(port=config['mgmt_port'])
        else:
            self.rmaster = redis.StrictRedis(host=self.r.info()['master_host'], port=config['mgmt_port'], socket_connect_timeout=1, socket_timeout=1)
        monaco.refresh(self.r)
        node_id = monaco.node_ids_by_hostname[config['hostname']]
        self.node = schema.MonacoNode(node_id=node_id)
        self.health_data = {} # dictionary of app_id -> DB health
        self.app_clients = {} # dictionary of app_id -> redis clients
        self.rps = redis.StrictRedis(port=config['mgmt_port'])
        self.pubsub = self.rps.pubsub(ignore_subscribe_messages=True)
        self.lock = threading.Lock()
        self._subscriptions = {}
        self.logger = logging.getLogger('monaco.slave')
        self.redmanager = RedisMgmt()
        self.nutmanager = NutMgmt()
        # for slave based health-checks
        self.sched = Scheduler(daemon=True)
        self.sched.start()
        self.sched.add_interval_job(self.node_health, seconds=5) # TODO: Tune
        self.health_check_pool = ThreadPool(10)
        atexit.register(lambda: self.sched.shutdown(wait=False))

    def node_health(self):
        '''
        Slave-side health checking for all apps on this node
        Updates state/offset for redis servers, providing visibility for master
        '''
        def app_health(app_id):
            ''' Reports the health and replication info of a Monaco DB '''
            # init so DB creation doesn't cause exception
            offset = 0
            try:
                app = schema.App(app_id=app_id)
                app.refresh(self.r)
                if app.app_id in self.app_clients:
                    appcli = self.app_clients[app_id]
                else:
                    appcli = redis.StrictRedis(port=app.port, socket_connect_timeout=1, socket_timeout=1)
                    self.app_clients[app_id] = appcli
                info = appcli.info()
                if info['role'] == 'slave':
                    offset = info['slave_repl_offset']
                # Reset 'strike-count'
                self.health_data[app_id] = 0
            except redis.TimeoutError, err:
                # TODO evaluate
                self.logger.warn('App %s had a timeout accessing DB.. doing nothing!', app.app_id)
                return False
            except redis.RedisError, err:
                self.logger.exception(err)
                # Account for miss, report if necessary
                if app_id in self.health_data:
                    self.health_data[app_id] += 1
                else:
                    self.health_data[app_id] = 1
                # Delete old app_client in case our local client went stale
                del self.app_clients[app_id]
            # separate the redis errors from accessing user db, from redis errors updating state
            try:
                if self.health_data[app_id] >= 3:
                    app.set_node_state(self.node.node_id, 'stop', self.rmaster)
                    return False
                else:
                    app.set_node_status(self.node.node_id, self.rmaster, state='run', offset=offset)
                    return True
            except (redis.ReadOnlyError, redis.ResponseError):
                # Relearn master on master change & ReadOnlyError
                # Scripts error response isn't properly parsed. Might fix in redis-py
                self.logger.info('Redis error updating app-state, possibly caused by recent Monaco Master failover')
                if self.r.info()['role'] == 'master':
                    self.rmaster = redis.StrictRedis(port=config['mgmt_port'], socket_connect_timeout=1, socket_timeout=1)
                else:
                    self.rmaster = redis.StrictRedis(self.r.info()['master_host'], port=config['mgmt_port'], socket_connect_timeout=1, socket_timeout=1)
            return False

        results, result = [], True
        self.node.refresh(self.r)
        for app_id in self.node.apps:
            results.append(self.health_check_pool.apply_async(app_health, (app_id,)))
        for data in results:
            try:
                result = result and data.get()
            except Exception, exc:
                self.logger.exception(exc)
        return result

    def update(self):
        '''
        Triggers an syncronization against the state stored in redis
        '''
        self.node_handler_factory()('')
        self.twem_handler_factory()('')

    def update_subs(self):
        '''
        Subscribes the slave to the channels associated with the
        '''
        try:
            with self.lock:
                channels = self._subscriptions.keys()

                subs = {}
                self.node.refresh(self.r)
                subs[keysub(schema.NODE_APPS_TMPL % self.node.node_id)] = self.node_handler_factory()
                subs[keysub(schema.NODE_TWEMS_TMPL % self.node.node_id)] = self.twem_handler_factory()
                for app_id in self.node.apps:
                    subs[keysub(schema.APP_CLUSTER_TMPL % app_id)] = self.app_handler_factory(app_id)
                    subs[keysub(schema.APP_HASH_TMPL % app_id)] = self.app_config_handler_factory(app_id)
                map(self._remove_subscription, [chan for chan in channels if chan not in subs.keys()])
                for k, v in subs.iteritems():
                    self._add_subscription(k, v)
        except redis.RedisError, err:
            self.logger.exception(err)
            self.r = redis.StrictRedis(port=config['mgmt_port'])

    def _add_subscription(self, channel, handler):
        '''
        Adds a new channel to the slave's listener
        EXPECTS self.lock to be acquired, this is NOT protected
        '''
        if channel in self._subscriptions:
            if self._subscriptions[channel] != handler:
                self.pubsub.subscribe(**{channel: handler})
                if not channel in self.pubsub.channels:
                    self.logger.error('Failed to subscribe to %s', channel)
                    raise RuntimeError('Failed to subscribe to %s' % channel)
                self._subscriptions[channel] = handler
        else:
            self.pubsub.subscribe(**{channel: handler})
            if not channel in self.pubsub.channels:
                self.logger.error('Failed to subscribe to %s',  channel)
                raise Exception('Failed to subscribe to %s' % channel)
            self._subscriptions[channel] = handler

    def _remove_subscription(self, channel):
        '''
        Removes a channel from the slave's listener
        EXPECTS self.lock to be acquired, this is NOT protected
        '''
        if channel in self._subscriptions:
            self.pubsub.unsubscribe(channel)
        del self._subscriptions[channel]

    def is_subscribed(self, channel):
        '''
        Returns bool whether the slave is subscribed to the channel
        '''
        with self.lock:
            return channel in self._subscriptions

    def start(self):
        '''
        start abstraction for redis-py's threaded listener
        '''
        with self.lock:
            if hasattr(self, '_listener_thread'):
                if self._listener_thread.is_alive():
                    return
            self._listener_thread = self.pubsub.run_in_thread(sleep_time=0.01)
        self.update()

    def stop(self):
        '''
        stop abstraction for redis-py's threaded listener
        '''
        with self.lock:
            if hasattr(self, '_listener_thread'):
                if self._listener_thread.is_alive():
                    self._listener_thread.stop()
                delattr(self, '_listener_thread')

    def is_alive(self):
        '''
        Returns bool whether the listener thread is alive
        '''
        with self.lock:
            return hasattr(self, '_listener_thread') and self._listener_thread.is_alive()

    def node_handler_factory(self):
        '''
        factories are no joking matter
        '''
        def handler(_):
            '''
            handler for when this node's mapping has been updated
            '''
            try:
                with self.lock:
                    self.node.refresh(self.r)
                    instances = self.redmanager.list_instances()
                    for app_id in instances:
                        if app_id not in self.node.apps:
                            self.redmanager.delete_instance(app_id)
                            app = schema.App(app_id=app_id)
                            app.clear_node_status(self.node.node_id, self.rmaster)
                    for app_id in self.node.apps:
                        app = schema.App(app_id=app_id)
                        app.refresh(self.r)
                        if app_id not in instances:
                            if self.new_app(app):
                                # just set it to 'run' optimistically. It will be corrected if necessary very fast
                                app.set_node_status(self.node.node_id, self.rmaster, revision=app.revision, state='run')
                                self.logger.debug('Created app:%s, revision:%s', app.app_id, app.revision)
                            else:
                                self.logger.error('Failed to create! node:%s, app:%s, revision:%s', self.node.node_id, app.app_id, app.revision)
                        else:
                            if self.update_app(app):
                                app.set_node_revision(self.node.node_id, app.revision, self.rmaster)
                                self.logger.debug('Updated app! node:%s, app:%s, revision:%s', self.node.node_id, app.app_id, app.revision)
                            else:
                                self.logger.error('Failed to update! node:%s, app:%s, revision:%s', self.node.node_id, app.app_id, app.revision)
                self.update_subs()
            except Exception, exc:
                self.logger.exception(exc)
        return handler

    def twem_handler_factory(self):
        ''' returns a handler function for twem set changes '''
        def handler(_):
            ''' handler for when the proxies on this node are modified '''
            try:
                with self.lock:
                    self.node.refresh(self.r)
                    # this will stop, reconf, and start nutcracker.
                    # should work 80% of the time, all of the time ;)
                    for twem_id in self.node.twems:
                        twem = schema.MonacoTwem(twem_id=twem_id)
                        twem.refresh(self.r)
                        self.nutmanager.update_twem(twem)
                    for twem_id in self.nutmanager.list_nutcracker_instances():
                        if not twem_id in self.node.twems:
                            self.nutmanager.delete_twem(twem_id)
            except Exception, exc:
                self.logger.exception(exc)
        return handler

    def app_handler_factory(self, app):
        '''
        returns a non-class method accessing class objects.. I could just pass a self.method probably
        '''
        if type(app) != schema.App:
            app = schema.App(app_id=app)
        def handler(_):
            '''
            handler for when an app this node manages has had is cluster modified
            '''
            try:
                if not app.exists(self.r):
                    return # already deleted if deleted
                app.refresh(self.r)
                if not self.node.node_id in app.nodes:
                    return # node handler will delete the app
                with self.lock:
                    if self.update_app(app):
                        app.set_node_revision(self.node.node_id, app.revision, self.rmaster)
                        self.logger.debug('Updated revision! node:%s, app:%s, revision:%s', self.node.node_id, app.app_id, app.revision)
                    else:
                        self.logger.error('Failed to update! node:%s, app:%s, revision:%s', self.node.node_id, app.app_id, app.revision)
            except Exception, exc:
                self.logger.exception(exc)
        return handler

    def app_config_handler_factory(self, app):
        ''' I still dislike factories - pylint :) '''
        if type(app) != schema.App:
            app = schema.App(app_id=app)
        def handler(_):
            '''
            handler for when users update config settings
            '''
            try:
                if not app.exists(self.r):
                    return # app deleted, this signal can be ignored
                app.refresh(self.r)
                if not self.node.node_id in app.nodes:
                    return # node handler will delete the app
                with self.lock:
                    if self.update_app(app):
                        app.set_node_revision(self.node.node_id, app.revision, self.rmaster)
                        self.logger.debug('Updated revision! node:%s, app:%s, revision:%s', self.node.node_id, app.app_id, app.revision)
                    else:
                        self.logger.error('Failed to update! node:%s, app:%s, revision:%s', self.node.node_id, app.app_id, app.revision)
            except Exception, exc:
                self.logger.exception(exc)
        return handler

    def new_app(self, app):
        ''' helper function for self.redmanager.create_instance '''
        return self.redmanager.create_instance(app, self.node.node_id, self.r)

    def update_app(self, app):
        '''
        Updates app with new conf, restarting if necessary
        '''
        if not self.node.node_id in app.nodes:
            self.logger.warn("App %s was deleted via 'udpate'", app.app_id)
            return self.redmanager.delete_instance(app.app_id)
        if not self.redmanager.instance_exists(app.app_id):
            self.logger.warn("App %s was 'updated' when it did not previously exist...", app.app_id)
            return self.new_app(app)
        if not self.redmanager.instance_alive(app.app_id):
            self.redmanager.restart_instance(app.app_id)
        return self.redmanager.update_instance(app, self.node.node_id, self.r)
