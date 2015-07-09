#!/usr/bin/python#
# coding=utf-8
'''
Daemon that reports stats for this MonacoNode's Apps
'''
from __future__ import print_function, division

import redis
import schema
import redismgmt
import time
import logging
import socket
import psutil
import threading
import json
import nutmgmt
import config

STATLOGGER = logging.getLogger('monaco.stats')

PUBLISHER = config.config['statpublisher']()

class MonacoHandler(threading.Thread):
    '''
    Thread that handles a specific app's stat reporting, for better cpu monitoring
    '''
    def __init__(self, node_id):
        threading.Thread.__init__(self)
        self.r = redis.StrictRedis(port=config.config['mgmt_port'])
        self.node_id = node_id
        self.interval = config.config['stats']['interval']
        self.threshold = 3
        self.failcount = 0
        self.logger = STATLOGGER
        self.redmgr = redismgmt.RedisMgmt()
        self._run = True

    def stop(self):
        ''' stop thread '''
        self._run = False

    def run(self):
        '''
        Runs until no longer valid (app moved, not master, etc), or repeated errors over self.threshold
        '''
        while self._run:
            start_time = time.time()
            if self.failcount >= self.threshold:
                self.logger.error('Node stat reporter thread for Monaco DB is sleeping to prevent thrashing')
                time.sleep(15)

            # Collect/Publish stats
            try:
                info = self.r.info()
                for stat in ['connected_slaves', 'used_memory', 'connected_clients', 'instantaneous_ops_per_sec']:
                    PUBLISHER.publish_monaco_stat(self.node_id, stat, info[stat])

                proc = self.redmgr.instance_proc('6379')
                if proc:
                    PUBLISHER.publish_monaco_stat(self.node_id, 'cpu_percent', proc.cpu_percent(interval=self.interval))

            except redis.ConnectionError:
                self.logger.info('Redis error accessing Monaco DB info')
                del self.r
                self.r = redis.StrictRedis(port=config.config['mgmt_port'])

            except psutil.Error, err:
                self.logger.info('psutil error - %s', repr(err))
                self.failcount += 1

            except Exception, exc:
                self.logger.exception(exc)
                self.failcount += 1

            spent_time = time.time() - start_time
            if self.interval > spent_time:
                time.sleep(self.interval - spent_time)

            self.logger.debug('monaco stat handler reporting cycle')
            self.failcount = 0

class AppHandler(threading.Thread):
    '''
    Thread that handles a specific app's stat reporting, for better cpu monitoring
    '''
    def __init__(self, app_id, node_id, threshold=3):
        threading.Thread.__init__(self)
        self.app_id = app_id
        self.app = schema.App(app_id=self.app_id)
        self.r = redis.StrictRedis(port=config.config['mgmt_port'])
        self.app.refresh(self.r)
        self.app_conn = self.app.get_master_connection(self.r)
        self.node_id = node_id
        self.interval = config.config['stats']['interval']
        self.threshold = threshold
        self.failcount = 0
        self.logger = STATLOGGER
        self.redmgr = redismgmt.RedisMgmt()
        self._run = True

    def stop(self):
        ''' stop thread '''
        self._run = False

    def run(self):
        '''
        Runs until no longer valid (app moved, not master, etc), or repeated errors over self.threshold
        '''
        while self._run:
            start_time = time.time()
            if self.failcount >= self.threshold:
                self.logger.warn('Node stat reporter thread for app %s is terminating from thrashing', self.app_id)
                return

            # Refresh App and verify our responsibility
            try:
                self.app.refresh(self.r)
                if not self.node_id in self.app.nodes or self.app.nodes[self.node_id] != 'master':
                    # No longer this node's responsibility to report stats
                    return
            except redis.RedisError:
                self.logger.info('RedisError updating app %s', self.app_id)
                del self.r
                self.r = redis.StrictRedis(port=config.config['mgmt_port'])
                self.failcount += 1
                continue
            except Exception, exc:
                self.logger.exception(exc)
                self.failcount += 1
                continue

            # Collect/Publish stats
            try:
                info = self.app_conn.info()
                for stat in ['connected_slaves', 'used_memory', 'connected_clients', 'instantaneous_ops_per_sec']:
                    PUBLISHER.publish_app_stat(self.app_id, stat, info[stat])

                proc = self.redmgr.instance_proc(self.app_id)
                if proc:
                    PUBLISHER.publish_app_stat(self.app_id, 'cpu_percent', proc.cpu_percent(interval=self.interval))

                PUBLISHER.publish_app_stat(self.app_id, 'master_unavailable', 0)

            except redis.ConnectionError:
                self.logger.info('Redis error accessing %s info', self.app_id)
                PUBLISHER.publish_app_stat(self.app_id, 'master_unavailable', 1)
                del self.app_conn
                self.app_conn = self.app.get_master_connection(self.r)

            except psutil.Error, err:
                self.logger.info('psutil error - %s', repr(err))
                time.sleep(self.interval) # since the wait time might not have been observed
                self.failcount += 1

            except Exception, exc:
                self.logger.exception(exc)
                self.failcount += 1

            spent_time = time.time() - start_time
            if self.interval > spent_time:
                time.sleep(self.interval - spent_time)

            self.logger.debug('stat handler(%s) reporting cycle', self.app_id)
            self.failcount = 0


class TwemHandler(threading.Thread):
    '''
    Thread that handles a specific proxy's stat reporting
    '''
    def __init__(self, twem_id, node_id, host, threshold=3):
        threading.Thread.__init__(self)
        self.twem_id = twem_id
        self.node_id = node_id
        self.twem = schema.MonacoTwem(twem_id=twem_id)
        self.host = host
        self.nutmgr = nutmgmt.NutMgmt()
        self.interval = config.config['stats']['interval']
        self.threshold = threshold
        self.failcount = 0
        self.logger = STATLOGGER
        self._run = True

    def get_twem_stat(self):
        '''
        Returns JSON-parsed data for self.twem.
        self.twem should be 'refreshed'
        '''
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('localhost', self.twem.stat_port))
            data = json.load(sock.makefile())
            return data[self.twem.name]
        except Exception:
            STATLOGGER.warn('Exception getting proxy stats for twem %s', self.twem.twem_id)
        return None

    def stop(self):
        ''' stops the thread '''
        self._run = False

    def run(self):
        '''
        Runs until no longer valid (app moved, not master, etc), or repeated errors over self.threshold
        '''
        self.r = redis.StrictRedis(port=config.config['mgmt_port'])
        while self._run:
            start_time = time.time()
            if self.failcount >= self.threshold:
                self.logger.warn('Node stat reporter thread for twem %s is terminating from thrashing', self.twem_id)
                return

            # Refresh App and verify our responsibility
            try:
                self.twem.refresh(self.r)
                if not self.node_id in self.twem.nodes:
                    # No longer this node's responsibility to report stats
                    return
                data = self.get_twem_stat()
                self.logger.debug(repr(data))
            except redis.RedisError:
                self.logger.info('RedisError updating twem %s', self.twem_id)
                self.r = redis.StrictRedis(port=config.config['mgmt_port'])
                self.failcount += 1
                continue
            except Exception, exc:
                self.logger.exception(exc)
                self.failcount += 1
                continue

            # Collect/Publish stats
            try:
                for stat in ['client_connections', 'client_err', 'forward_error']:
                    PUBLISHER.publish_twem_stat(self.twem_id, self.host, stat, data[stat])

                proc = self.nutmgr.nutcracker_instance_proc(self.twem)
                if proc:
                    PUBLISHER.publish_twem_stat(self.twem_id, self.host, 'cpu_percent', proc.cpu_percent(interval=self.interval))
                PUBLISHER.publish_twem_stat(self.twem_id, self.host, 'host_unavailable', 0)

            except psutil.Error, err:
                self.logger.info('psutil error - %s', repr(err))
                time.sleep(self.interval) # since the wait time might not have been observed
                self.failcount += 1

            except Exception, exc:
                self.logger.exception(exc)
                PUBLISHER.publish_twem_stat(self.twem_id, self.host, 'host_unavailable', 1)
                self.failcount += 1

            spent_time = time.time() - start_time
            if self.interval > spent_time:
                time.sleep(self.interval - spent_time)

            self.logger.debug('stat handler(%s) reporting cycle', self.twem_id)
            self.failcount = 0


def main():
    '''
    This is a jazzier version of the node stats reporter.
    It will spin up N threads (where N = the number of app Masters on this node)
    Those threads will report stats on the config interval
    '''
    r = redis.StrictRedis(port=config.config['mgmt_port'])
    monaco = schema.Monaco()
    monaco.refresh(r)
    host = config.config['hostname']
    node_id = monaco.node_ids_by_hostname[host]
    node = schema.MonacoNode(node_id=node_id)
    monaco_handler = MonacoHandler(node_id)
    monaco_handler.start()

    app_threadmap = {}
    twem_threadmap = {}
    while True:
        try:
            node.refresh(r)

            # Set up this node's master DB handlers
            for app_id in app_threadmap.keys():
                if app_id not in node.apps:
                    # child thread should die a natural, painless death
                    app_threadmap[app_id].stop()
                    del app_threadmap[app_id]
                    STATLOGGER.debug('deleted %s', app_id)
            for app_id in node.apps:
                app = schema.App(app_id=app_id)
                app.refresh(r)
                if app.nodes[node.node_id] != 'master':
                    if app_id in app_threadmap:
                        app_threadmap[app_id].stop()
                        del app_threadmap[app_id]
                        STATLOGGER.debug('deleted %s', app_id)
                    continue
                if not app_id in app_threadmap:
                    # perhaps a new thing
                    app_threadmap[app_id] = AppHandler(app_id, node_id)
                    app_threadmap[app_id].start()
                    STATLOGGER.debug('started %s', app_id)
                elif not app_threadmap[app_id].is_alive():
                    del app_threadmap[app_id]
                    app_threadmap[app_id] = AppHandler(app_id, node_id)
                    app_threadmap[app_id].start()
                    STATLOGGER.info('restarted %s', app_id)

            # Set up this node's twem handlers
            for twem_id in twem_threadmap.keys():
                if twem_id not in node.twems:
                    # child thread should die a natural, painless death
                    twem_threadmap[twem_id].stop()
                    del twem_threadmap[twem_id]
                    STATLOGGER.debug('deleted %s', twem_id)
            for twem_id in node.twems:
                twem = schema.MonacoTwem(twem_id=twem_id)
                twem.refresh(r)
                if not twem_id in twem_threadmap:
                    # perhaps a new thing
                    twem_threadmap[twem_id] = TwemHandler(twem_id, node_id, host)
                    twem_threadmap[twem_id].start()
                    STATLOGGER.debug('started %s', twem_id)
                elif not twem_threadmap[twem_id].is_alive():
                    del twem_threadmap[twem_id]
                    twem_threadmap[twem_id] = TwemHandler(twem_id, node_id, host)
                    twem_threadmap[twem_id].start()
                    STATLOGGER.info('restarted %s', twem_id)
        except redis.RedisError:
            r = redis.StrictRedis(port=config.config['mgmt_port'])
        except Exception, exc:
            STATLOGGER.exception(exc)

        time.sleep(5)


if __name__ == '__main__':
    config.initLoggers()
    main()
