#!/usr/bin/python
# coding=utf-8
'''
Provides an API for managing redis instances
'''

from __future__ import print_function, division

import jinja2
import os
import subprocess
import re
import threading
import logging
import psutil
import redis

from config import config
import schema

REDISMGMTLOCK = threading.Lock()

def human2bytes(string):
    '''
    Takes a human readable size suffixed with kb,mb, or gb, and returns the int number of bytes
    '''
    if string[-2:].lower() == 'kb':
        return int(string[:-2]) * 1024
    if string[-2:].lower() == 'mb':
        return int(string[:-2]) * 1024 * 1024
    if string[-2:].lower() == 'gb':
        return int(string[:-2]) * 1024 * 1024 * 1024

def bytes2human(numbytes):
    '''
    Converts a size in bytes to kb,mb,gb depending on most appropriate scale
    '''
    numbytes = int(numbytes) // 1024
    if numbytes < 1024:
        return '%skb' % numbytes
    numbytes = numbytes // 1024
    if numbytes < 1024:
        return '%smb' % numbytes
    return '%sgb' % (numbytes // 1024)

class RedisMgmt(object):
    '''
    This class manages the configuration and state of a redis instance
    '''
    def __init__(self):
        self.logger = logging.getLogger('monaco.RedisMgmt')
        self.lock = REDISMGMTLOCK
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'templates/redis.conf')) as template_file:
            self.template = jinja2.Template(template_file.read())


        self.TEMPLATED = ['port', 'rename-config', 'rename-shutdown']
        self.NOT_DYNAMIC = ['maxmemory']
        self.DYNAMIC = ['maxmemory-policy', 'save', 'slaveof', 'slave-read-only']

    def app_conf_struct(self, app, node_id, r):
        '''
        Given a schema.App, node_id, and redis cli; this will return the dict
        '''
        if type(app) != schema.App:
            app = schema.App(app_id=app)
            app.refresh(r)
        if type(node_id) == schema.MonacoNode:
            node_id = node_id.node_id
        node_id = str(node_id)

        # facepalm. it keeps the config value internally as bytes
        # but on config_rewrite, redis writes in human readable.

        struct = {
            'port': app.port,
            'rename_config': app.translate('config'),
            'rename_shutdown': app.translate('shutdown'),
            'maxmemory': bytes2human(app.maxmemory),
            'maxmemory-policy': app.maxmemory_policy,
            'slave-read-only': 'yes'
        }

        if app.persist == 'True':
            struct['save'] = '3600 10'
        else:
            struct['save'] = ''

        if app.nodes[node_id] != 'master':
            if app.nodes[node_id] == 'slave-write':
                struct['slave-read-only'] = 'no'
            for node_id, role in app.nodes.iteritems():
                if role == 'master':
                    node = schema.MonacoNode(node_id=node_id)
                    node.refresh(r)
                    struct['slaveof'] = '%s %s' % (node.FQDN, app.port)
                    break
            # This is intentionally catching clean for loop exit
            else:
                raise RuntimeError('No master_host could be identified')
        else:
            struct['slaveof'] = 'no one'
        return struct

    def app_conf(self, struct):
        '''
        Renders the conf struct into a redis config
        '''
        conf = self.template.render(struct)
        for var in self.NOT_DYNAMIC + self.DYNAMIC:
            if var == 'slaveof' and struct[var] == 'no one':
                continue
            if var == 'save' and not var in struct:
                continue
            conf += '\n%s %s' % (var, struct[var])
        return conf

    def read_conf(self, app_id):
        '''
        Reads the app config for the specified app returning (prefix, {dynamic: vars})
        '''
        filename = 'monaco-%s.conf' % app_id
        filepath = os.path.join(config['monaco']['monaco-app-confs'], filename)
        with open(filepath) as conffile:
            conf = conffile.read()
        return conf

    def parse_conf(self, app_id):
        ''' Parses a monaco redis config '''
        prefix = ''
        dynamic = {}
        conf = self.read_conf(app_id)
        flag = False
        for line in conf.splitlines():
            if line.startswith('##### DYNAMIC #####'):
                flag = True
            elif flag:
                if line.startswith('#'):
                    # redis.rewrite leaves a comment about it not making any unecessary changes... :|
                    continue
                parts = line.split()
                if len(parts) < 2:
                    # blank line
                    continue
                dynamic[parts[0]] = ' '.join(parts[1:])
            else:
                prefix += line
        if not flag:
            raise RuntimeError("Flag '##### DYNAMIC #####' not found in conf (%s)" % app_id)

        return (prefix, dynamic)

    def create_instance(self, app, node_id, r):
        '''
        Creates the given app
        '''
        with self.lock:
            conf = self.app_conf(self.app_conf_struct(app, node_id, r))
            filename = 'monaco-%s.conf' % app.app_id
            filepath = os.path.join(config['monaco']['monaco-app-confs'], filename)
            with open(filepath, 'w') as conffile:
                conffile.write(conf)
            os.chmod(filepath, 0666)

            cmd = 'sudo start monaco-db N=%s' % app.app_id
            proc = subprocess.Popen(
                cmd.split(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            (out, err) = proc.communicate()
            if proc.wait() != 0:
                os.remove(filepath)
                print(out, err)
                self.logger.error('Failed to start app %s\n%s\n%s', app.app_id, out, err)
                return False
        self.logger.info('Created app %s', app.app_id)
        return True

    def restart_instance(self, app_id):
        '''
        Performs a service restart for the specified app
        '''
        with self.lock:
            # Restart
            cmd = 'sudo restart monaco-db N=%s' % app_id
            proc = subprocess.Popen(
                cmd.split(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            (out, err) = proc.communicate()
            if proc.wait() == 0:
                self.logger.info('Restarted app %s', app_id)
                return True
            self.logger.info('Failed to restart app %s\n%s\n%s', app_id, out, err)

            # Start in case the process was stopped
            cmd = 'sudo start monaco-db N=%s' % app_id
            proc = subprocess.Popen(
                cmd.split(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            (out, err) = proc.communicate()
            if proc.wait() != 0:
                self.logger.error('Failed to start app %s\n%s\n%s', app_id, out, err)
                return False

        self.logger.info('Started app %s', app_id)
        return True

    def update_instance(self, app, node_id, r, restart=True):
        '''
        Updates the app instance, rewriting the config, and restarting as necessary
        if restart=False, it won't restart the instance, even if needed to deploy changes.
        '''
        with self.lock:
            conf_struct = self.app_conf_struct(app, node_id, r)
            conf = self.app_conf(conf_struct)
            try:
                _, dynamic = self.parse_conf(app.app_id)
            except Exception, exc:
                self.logger.exception(exc)
                self.logger.error('Unable to parse existing config file, so rewriting. App (%s) is potentially in different state to file!', app.app_id)
                try:
                    filename = 'monaco-%s.conf' % app.app_id
                    filepath = os.path.join(config['monaco']['monaco-app-confs'], filename)
                    with open(filepath, 'w') as conffile:
                        conffile.write(conf)
                    os.chmod(filepath, 0666)
                except Exception, exc2:
                    self.logger.exception(exc2)
                    self.logger.error('Error manually rewriting the config. App (%s) is potentially unable to restart', app.app_id)
                return False

            changed = False
            changes = {}
            for key in self.NOT_DYNAMIC:
                if key == 'maxmemory' and key in dynamic and key in conf_struct:
                    # edge-case: increasing maxmemory doesn't require restart
                    if human2bytes(dynamic[key]) > human2bytes(conf_struct[key]):
                        changed = True
                    elif dynamic[key] != conf_struct[key]:
                        # config set maxmemory takes int only?!?
                        changes[key] = human2bytes(conf_struct[key])
                elif key in dynamic:
                    if key in conf_struct and dynamic[key] != conf_struct[key]:
                        changed = True
                elif key in conf_struct:
                    changed = True
            for key in self.DYNAMIC:
                if key in dynamic:
                    if key in conf_struct and dynamic[key] != conf_struct[key]:
                        changes[key] = conf_struct[key]
                elif key in conf_struct:
                    if key == 'slaveof' and conf_struct['slaveof'] == 'no one':
                        continue
                    if key == 'save' and conf_struct['save'] == '':
                        continue
                    changes[key] = conf_struct[key]

            if not changed:
                # everything changed can be set via redis.config_set
                if len(changes) == 0:
                    self.logger.debug('App %s is properly configured', app.app_id)
                    return True
                try:
                    appcli = app.get_local_admin_cli()
                    for key, val in changes.iteritems():
                        if key == 'slaveof':
                            if val == 'no one':
                                changed |= not appcli.slaveof()
                            else:
                                changed |= not appcli.slaveof(*val.split())
                            self.logger.info('Changed replication role to %s for app %s', appcli.info()['role'], app.app_id)
                            continue
                        appcli.config_set(key, val)
                    appcli.config_rewrite()
                    self.logger.info('Rewrote config file, performed live changes for app %s', app.app_id)
                except redis.RedisError, err:
                    self.logger.exception(err)
                    changed = True
                # double check
                _, dynamic = self.parse_conf(app.app_id)
                for key in self.NOT_DYNAMIC + self.DYNAMIC:
                    if not key in conf_struct:
                        continue
                    if not key in dynamic and key == 'slaveof' and conf_struct['slaveof'] == 'no one':
                        # since redis deletes the slaveof line on config rewrite for masters
                        continue
                    if not key in dynamic and key == 'save' and conf_struct['save'] == '':
                        # too many goddamn edge cases.. 
                        continue
                    if dynamic[key] != conf_struct[key]:
                        # set changed to manually overwrite the config
                        changed = True
                        self.logger.info('App %s failed to dynamically reconfig', app.app_id)
                        self.logger.info('Key (%s) mismatch: "%s" vs. "%s"', key, dynamic[key], conf_struct[key])
                        break
                else:
                    if not changed:
                        # no issues
                        return True

            if changed:
                try:
                    # something that can't be updated on the fly has been changed
                    filename = 'monaco-%s.conf' % app.app_id
                    filepath = os.path.join(config['monaco']['monaco-app-confs'], filename)
                    with open(filepath, 'w') as conffile:
                        conffile.write(conf)
                    os.chmod(filepath, 0666)
                    self.logger.info('Replaced config file for app %s', app.app_id)
                except Exception, exc:
                    self.logger.error('Failed to update config for app %s, potentially corrupting state!!!', app.app_id)
                    self.logger.exception(exc)
                    return False

        # useful for persisting master/slave role, but updating running process via. redis
        if restart and changed:
            if self.restart_instance(app.app_id):
                self.logger.info('Updated & Restarted app %s', app.app_id)
                return True
            self.logger.warn('Updated config, but failed to restart app %s', app.app_id)
            return False
        return True

    def delete_instance(self, app_id):
        '''
        Halts the instance, and deletes the config
        '''
        with self.lock:
            check = False
            cmd = 'sudo stop monaco-db N=%s' % app_id
            proc = subprocess.Popen(
                cmd.split(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            (out, err) = proc.communicate()
            if proc.wait() != 0:
                self.logger.error('Failed to stop app %s\n%s\n%s', app_id, out, err)
                check = True

            filename = 'monaco-%s.conf' % app_id
            filepath = os.path.join(config['monaco']['monaco-app-confs'], filename)
            if os.path.isfile(filepath):
                os.remove(filepath)
            if check and self.instance_alive(app_id):
                app = self.instance_proc(app_id)
                if not app:
                    self.logger.error('App %s is still running, but psutil failed to locate the process')
                else:
                    cmd = 'kill -9 %s' % app.pid()
                    proc = subprocess.Popen(
                        cmd.split(),
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                    (out, err) = proc.communicate()
                    if proc.wait() != 0:
                        self.logger.error('Failed to kill -9 app %s\n%s\n%s', app_id, out, err)
                    else:
                        self.logger.info('Killed (kill -9) %s (%s).', app_id, app.pid())
        self.logger.info('Deleted app %s', app_id)
        return True

    def instance_alive(self, app_id):
        '''
        Returns the running status of the instance
        '''
        app_pattern = re.compile(r'0\.0\.0\.0:%s' % app_id)
        with self.lock:
            for proc in psutil.process_iter():
                if proc.name() == 'redis-server' and proc.ppid() == 1:
                    if app_pattern.search(proc.cmdline()[0]):
                        return True
        return False

    def instance_exists(self, app_id):
        '''
        Returns whether a conf exists for this app_id
        '''
        return app_id in self.list_instances()

    def instance_proc(self, app_id):
        '''
        Returns the psutil.proc of the process serving app_id
        Returns None on bad arguments/state
        '''
        app_pattern = re.compile(r'0\.0\.0\.0:%s' % app_id)
        try:
            with self.lock:
                for proc in psutil.process_iter():
                    if proc.name() == 'redis-server' and proc.ppid() == 1:
                        if app_pattern.search(proc.cmdline()[0]):
                            return proc
        except Exception, exc:
            self.logger.warn('instance_proc(app_id=%s) failed:\n%s', app_id, repr(exc))
        return None

    def list_instances(self):
        '''
        returns a list of configured instances
        '''
        with self.lock:
            instances = []
            r = re.compile(r'monaco-([0-9]*)\.conf')
            for conffile in os.listdir(config['monaco']['monaco-app-confs']):
                match = r.search(conffile)
                if match:
                    instances.append(match.group(1))
        return instances

    def verify_instances(self):
        '''
        returns ([instances that should be but aren't running, ...], [instances that shouldn't be but are running, ...])
        '''
        extra_instances = []
        app_pattern = re.compile(r'0\.0\.0\.0:([0-9]*)')
        expected_instances = self.list_instances()
        with self.lock:
            for proc in psutil.process_iter():
                if proc.name() == 'redis-server' and proc.ppid() == 1:
                    match = app_pattern.search(proc.cmdline()[0])
                    if match and match.group(1) != '6379':
                        instance = match.group(1)
                        if not instance in expected_instances:
                            extra_instances.append(instance)
                        elif instance in expected_instances:
                            expected_instances.remove(instance)
        return (expected_instances, extra_instances)
