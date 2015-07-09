#!/usr/bin/python
# coding=utf-8
'''
Common methods for Monaco's UI
'''
from __future__ import absolute_import, print_function, division

from flask import Flask
from redis import StrictRedis, TimeoutError
import requests
import schema
from config import config

app = Flask(__name__)

# Create conf 'object' which has all the keys in
for key,val in config.iteritems():
    if key == 'raft':
        app.config.update(
            RAFT_CLUSTER=val['cluster'],
            RAFT_PORT=val['port'],
        )
    app.config[key.upper()] = val

app.config.update(
    MONACO_MAXMEMORY_OPTIONS = {
        '32mb': 32 * 1024 * 1024,
        '64mb': 64 * 1024 * 1024,
        '128mb': 128 * 1024 * 1024,
        '256mb': 256 * 1024 * 1024,
        '512mb': 512 * 1024 * 1024,
        '1gb': 1024 * 1024 * 1024,
        '2gb': 2 * 1024 * 1024 * 1024,
        '4gb': 4 * 1024 * 1024 * 1024,
    },
    MONACO_REPLICA_OPTIONS = [
        '1',
        '2',
        '3',
    ],
    MONACO_MAXMEMORY_POLICY_OPTIONS = [
        'volatile-lru',
        'allkeys-lru',
        'volatile-random',
        'allkeys-random',
        'volatile-ttl',
        'noeviction',
    ],
    MONACO_PROXY_DISTRIBUTIONS = [
        'ketama',
        'modula',
        'random',
    ],
)

def rediscli(master=True):
    '''get mgmt connection'''
    for hostname in app.config.get('MONACO_HOSTS', ['localhost']):
        try:
            r = StrictRedis(host=hostname, port=app.config['MGMT_PORT'], socket_connect_timeout=1)
            info = r.info()
            if info['role'] == 'master' or not master:
                return r
            else:
                return StrictRedis(host=info['master_host'], port=app.config['MGMT_PORT'], socket_connect_timeout=1)
        except TimeoutError:
            continue
    raise TimeoutError('Timeout connecting to monaco mgmt db')

def monaco_admin(func):
    ''' an important qualifier for admin access.. this method doesn't need a docstring '''
    def ret():
        ''' string of dox '''
        return func()
    return ret

# helper methods
def list_apps():
    ''' returns all app_ids '''
    r = rediscli()
    monaco = schema.Monaco()
    monaco.refresh(r)
    return monaco.app_ids

def list_apps_by_operator(groups):
    ''' lists all apps operated by one of the groups '''
    r = rediscli()
    monaco = schema.Monaco()
    monaco.refresh(r)
    apps = []
    for app_id in monaco.app_ids:
        try:
            dbapp = schema.App(app_id=app_id)
            dbapp.refresh(r)
            if dbapp.operator in groups:
                apps.append(app_id)
        except Exception:
            pass
    return apps

def list_apps_by_owner(owner):
    ''' list all apps where owner == app.owner '''
    r = rediscli()
    monaco = schema.Monaco()
    monaco.refresh(r)
    apps = []
    for app_id in monaco.app_ids:
        try:
            dbapp = schema.App(app_id=app_id)
            dbapp.refresh(r)
            if dbapp.owner == owner:
                apps.append(app_id)
        except Exception:
            pass
    return apps

def list_twems():
    ''' returns all twem_ids '''
    r = rediscli()
    monaco = schema.Monaco()
    monaco.refresh(r)
    return monaco.twem_ids

def list_twems_by_operator(groups):
    ''' lists all twems operated by one of the groups '''
    r = rediscli()
    monaco = schema.Monaco()
    monaco.refresh(r)
    twems = []
    for twem_id in monaco.twem_ids:
        try:
            twem = schema.MonacoTwem(twem_id=twem_id)
            twem.refresh(r)
            if twem.operator in groups:
                twems.append(twem_id)
        except Exception:
            pass
    return twems

def list_twems_by_owner(owner):
    ''' list all twem_ids where owner == twem.owner '''
    r = rediscli()
    monaco = schema.Monaco()
    monaco.refresh(r)
    twems = []
    for twem_id in monaco.twem_ids:
        try:
            twem = schema.MonacoTwem(twem_id=twem_id)
            twem.refresh(r)
            if twem.owner == owner:
                twems.append(twem_id)
        except Exception:
            pass
    return twems

import web.views
import web.api
import web.stats
