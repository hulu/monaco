#!/usr/bin/python
# coding=utf-8
'''
nutmgmt.py manages nuts. nutcrackers.
okay, only one nutcracker, but that's all you really need.
'''

from __future__ import print_function, division

import subprocess
import psutil
import yaml
import threading
import redis
import logging
import re
import os

from config import config
import schema

# module wide, we don't want multiple things start/stopping
NUTMGMTLOCK = threading.Lock()

class NutMgmt(object):
    '''
    This class contains the functions required to manage nutcracker
    '''
    # all possible conf keys for nutcracker
    CONF_KEYS = ['listen', 'hash', 'hash_tag', 'distribution', 'auto_eject_hosts', 'redis', 'server_retry_timeout', 'server_failure_limit', 'servers', 'timeout']
    CONF_BASEPATH = '/etc/nutcracker'
    CONF_FILE_TMPL = 'nutcracker-%s.yml'

    def __init__(self):
        self.r = redis.StrictRedis(port=config['mgmt_port'])
        self.logger = logging.getLogger('monaco.nutmgmt')
        self.lock = NUTMGMTLOCK

    def twem_conf_struct(self, twem, retry=True):
        '''
        Given a schema.MonacoTwem,
        returns the nutcracker config for that proxy in dict form
        '''
        try:
            if type(twem) != schema.MonacoTwem:
                twem = schema.MonacoTwem(twem_id=twem)
                twem.refresh(self.r)
            conf = {}
            for key in schema.MonacoTwem.HASH_KEYS:
                if hasattr(twem, key) and key in self.CONF_KEYS:
                    conf[key] = getattr(twem, key)
            conf['listen'] = twem.listen
            conf['auto_eject_hosts'] = twem.auto_eject_hosts
            conf['redis'] = True
            conf['servers'] = []

            if len(twem.servers) == 1:
                # configure to proxy across the master and slaves of a single monaco db, using physical hostnames
                app = schema.App(app_id=twem.servers[0])
                app.refresh(self.r)
                monaco = schema.Monaco()
                monaco.refresh(self.r)
                for node_id in app.nodes:
                    node = schema.MonacoNode(node_id=node_id)
                    node.refresh(self.r)
                    conf['servers'].append('%s:%s:1' % (node.FQDN, app.port))
            else:
                # configure to proxy across a set of monaco dbs, using the loadbalanced hostname
                for app_id in twem.servers:
                    conf['servers'].append('%s:%s:1' % (config['loadbalancer']['hostname'], app_id))
                # Allow for external servers that are manually specified
                if twem.extservers:
                    for server in twem.extservers:
                        conf['servers'].append('%s:1' % server)
            return {twem.name: conf}
        except redis.RedisError, err:
            self.r = redis.StrictRedis(port=config['mgmt_port'])
            if retry:
                return self.twem_conf_struct(twem, retry=False)
            else:
                self.logger.exception(err)


    def rewrite_twem_conf(self, twem):
        '''
        Given a conf struct, render the config file
        Return True if config changed
        '''
        with self.lock:
            filepath = os.path.join(self.CONF_BASEPATH, self.CONF_FILE_TMPL % twem.port)
            struct = self.twem_conf_struct(twem)
            try:
                conf = yaml.load(open(filepath))
                for key, val in struct[twem.name].iteritems():
                    if conf[twem.name][key] != val:
                        changed = True
                        break
                else:
                    changed = False
            except Exception:
                # conf file isn't right
                changed = True

            if not changed:
                self.logger.info('Twem %s is properly configured', twem.port)
                return False

            try:
                yaml.dump(struct, open(filepath, 'w'), default_flow_style=False)
                self.logger.info('Rewrote config for twem %s', twem.port)
            except Exception, exc:
                self.logger.exception(exc)
        return True

    def start_nutcracker_instance(self, port):
        '''
        Starts nutcracker
        returns True on success
        '''
        with self.lock:
            proc = subprocess.Popen(
                ['sudo', 'start', 'nutcracker', 'PORT=%s' % port],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            out, err = proc.communicate()
            if proc.wait() != 0:
                self.logger.warn('Failed to start nutcracker instance %s!\n%s\n%s', port, out, err)
                return False
            self.logger.info('Started nutcracker instance %s', port)
        return True

    def stop_nutcracker_instance(self, port):
        '''
        Stops nutcracker
        returns True on success
        '''
        with self.lock:
            proc = subprocess.Popen(
                ['sudo', 'stop', 'nutcracker', 'PORT=%s' % port],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            out, err = proc.communicate()
            if proc.wait() != 0:
                self.logger.warn('Failed to stop nutcracker instance %s!\n%s\n%s', port, out, err)
                return False
        self.logger.info('Stopped nutcracker instance %s', port)
        return True

    def status_nutcracker_instance(self, port):
        '''
        Stops nutcracker
        returns True on start/running, False on stopped
        '''
        with self.lock:
            proc = subprocess.Popen(['sudo', 'status', 'nutcracker', 'PORT=%s' % port])
            if proc.wait() != 0:
                self.logger.debug('Nutcracker instance %s is stopped', port)
                return False
        self.logger.debug('Nutcracker instance %s is started', port)
        return True

    def restart_nutcracker_instance(self, port):
        '''
        Restarts nutcracker
        '''
        self.stop_nutcracker_instance(port)
        return self.start_nutcracker_instance(port)

    def list_nutcracker_instances(self):
        '''
        returns a list of configured instances
        '''
        with self.lock:
            instances = []
            regex = re.compile(r'nutcracker-([0-9]*)\.yml')
            for conffile in os.listdir(self.CONF_BASEPATH):
                match = regex.search(conffile)
                if match:
                    instances.append(match.group(1))
        return instances

    def update_twem(self, twem):
        '''
        Given a schema.MonacoTwem
        Ensure the nutcracker instance for this proxy is properly configured
        returns True on success
        '''
        if self.rewrite_twem_conf(twem) or not self.status_nutcracker_instance(twem.port):
            # there was a config change
            return self.restart_nutcracker_instance(twem.port)
        return True

    def delete_twem(self, twem_id):
        ''' Given a twem_id, stop nutcracker and delete config '''
        self.stop_nutcracker_instance(twem_id)
        with self.lock:
            filepath = os.path.join(self.CONF_BASEPATH, self.CONF_FILE_TMPL % twem_id)
            os.remove(filepath)

    def nutcracker_instance_proc(self, twem):
        '''
        Returns the psutil.proc of a MonacoTwem, or None
        '''
        for proc in psutil.process_iter():
            if proc.name() == 'nutcracker' and proc.ppid() == 1 and str(twem.stat_port) in proc.cmdline():
                return proc
        self.logger.info('No instance found for twem %s', twem.twem_id)
        return None
