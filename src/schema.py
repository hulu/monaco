#!/usr/bin/python
# coding=utf-8
'''
Redis mgmt communication layer
'''
from __future__ import print_function, division

import redis
import pickle
import os
import time
import logging
import random

# Key naming schema                             # redis data-type
# class Monaco
# TODO: refactor all the id->name maps into class hashes
MONACO_NIBH = 'monaco-nodeid-by-hostname'       # hash
MONACO_AIBS = 'monaco-appid-by-service'         # hash
MONACO_HBNI = 'monaco-hostname-by-nodeid'       # hash
MONACO_SBAI = 'monaco-service-by-appid'         # hash
MONACO_TIBN = 'monaco-twemid-by-name'           # hash
MONACO_NBTI = 'monaco-name-by-twemid'           # hash
MONACO_NODES = 'monaco-nodes'                   # set
MONACO_APPS = 'monaco-apps'                     # set
MONACO_TWEMS = 'monaco-twems'                   # set
MONACO_NEXT_APP_ID = 'monaco-next-app-id'       # int-like key (incr'able)
MONACO_NEXT_TWEM_ID = 'monaco-next-twem-id'     # int-like key (incr'able)
# class MonacoJobQueue
MONACO_JOBS = 'monaco-jobs'                     # hash
MONACO_JOB_QUEUE = 'monaco-job-queue'           # list
MONACO_JOB_STATUS = 'monaco-job-status'         # hash
MONACO_JOB_OUTPUT_TMPL = 'monaco-job-output-%s' # key
# class MonacoNode (%s = node_id)
NODE_HASH_TMPL = 'monaco-node-%s'               # hash
NODE_APPS_TMPL = 'monaco-node-%s-apps'          # set
NODE_TWEMS_TMPL = 'monaco-node-%s-twems'        # set
# class App (%s = app_id)
APP_HASH_TMPL = 'monaco-app-%s'                 # hash
APP_CLUSTER_TMPL = 'monaco-app-%s-cluster'      # hash
APP_NODESTATUS_TMPL = 'monaco-app-%s-status'    # hash
APP_NODEVERSION_TMPL = 'monaco-app-%s-version'  # hash

# class MonacoTwem (%s = twem_id)
TWEM_HASH_TMPL = 'monaco-twem-%s'               # hash
TWEM_CLUSTER_TMPL = 'monaco-twem-%s-servers'    # set
TWEM_NODES_TMPL = 'monaco-twem-%s-nodes'        # set
TWEM_EXTSRV_TMPL = 'monaco-twem-%s-extservers'  # set

# Constants
MIN_APP_ID = 6380
MAX_APP_ID = 16379
MIN_TWEM_ID = 16380
MAX_TWEM_ID = 26379

def keysub(key):
    ''' returns a keyspace subscription channel for a specific key '''
    return '__keyspace@0__:%s' % key

def hash_update(key, hashdata, r):
    '''
    Atomically update the redis hash at <key>
    Set the values from the dict <hashdata>, using client <r>
    '''
    if hasattr(hash_update, 'script'):
        try:
            hash_update.script(args=[key] + [item for tup in hashdata.items() for item in tup], client=r)
            return
        except redis.RedisError:
            pass
    path = os.path.join(os.path.dirname(__file__), 'lua/hash_update.lua')
    with open(path) as scriptfile:
        script = scriptfile.read()
    hash_update.script = r.register_script(script)
    hash_update.script(args=[key] + [item for tup in hashdata.items() for item in tup], client=r)

def hash_write(key, hashdata, r):
    '''
    Atomically overwrite the redis hash at <key>
    Setting it equal to the dict <hashdata>, using client <r>
    '''
    if hasattr(hash_write, 'script'):
        try:
            hash_write.script(args=[key] + [item for tup in hashdata.items() for item in tup], client=r)
            return
        except redis.RedisError:
            pass
    path = os.path.join(os.path.dirname(__file__), 'lua/hash_write.lua')
    with open(path) as scriptfile:
        script = scriptfile.read()
    hash_write.script = r.register_script(script)
    hash_write.script(args=[key] + [item for tup in hashdata.items() for item in tup], client=r)


class MonacoJobHandler(logging.Handler):
    '''
    A logging.Handler which logs to the MonacoJobQueue output field
    for the JID this is initialized with. All output has a 10 minute expiry
    so this should not be used for persistent logging.
    '''
    def __init__(self, jid, r, *args, **kwargs):
        logging.Handler.__init__(self, *args, **kwargs)
        self.key = MONACO_JOB_OUTPUT_TMPL % jid
        self.r = r

    def emit(self, record):
        msg = self.format(record) + '\n'
        with self.r.pipeline() as pipe:
            pipe.append(self.key, msg)
            pipe.expire(self.key, 10*60) # 10 min expiry
            pipe.execute()


class MonacoJobQueue(object):
    '''
    The interface for Monaco's job queueing and reporting
    '''
    def __init__(self, r):
        self.r = r

    @property
    def empty(self):
        ''' returns True if empty '''
        return self.r.llen(MONACO_JOB_QUEUE) == 0

    def pending_jobs(self, r=None):
        ''' returns the number of pending jobs '''
        if not r:
            r = self.r
        return r.llen(MONACO_JOB_QUEUE)

    def get_next_job(self, r=None):
        '''
        returns the jid and job dict as a tuple for the next job in the pending queue
        sets status to running
        '''
        if not r:
            r = self.r
        jid = r.lpop(MONACO_JOB_QUEUE)
        r.hset(MONACO_JOB_STATUS, jid, 'R:%s' % time.time())
        return (jid, pickle.loads(r.hget(MONACO_JOBS, jid)))

    def status(self, jid, r=None):
        ''' does stuff '''
        if not r:
            r = self.r
        status = r.hget(MONACO_JOB_STATUS, jid)
        timestamp = float(status.split(':')[1])
        if status[0] == 'P':
            return {
                'status': 'pending',
                'timestamp': timestamp,
            }
        elif status[0] == 'R':
            return {
                'status': 'running',
                'timestamp': timestamp,
                'output': r.get(MONACO_JOB_OUTPUT_TMPL % jid),
            }
        elif status[0] == 'F':
            return {
                'status': 'finished',
                'timestamp': timestamp,
                'output': r.get(MONACO_JOB_OUTPUT_TMPL % jid),
                'result': status[1] == '+',
            }
        else:
            raise RuntimeError('Invalid status type for job %s: %s' % (jid, status))

    def pushback_job(self, job, r=None):
        '''
        queues a job. see master.API_FUNCTIONS
        returns JID
        '''
        if not r:
            r = self.r
        job = pickle.dumps(job)
        for _ in xrange(3):
            jid = ''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890') for _ in xrange(16))
            if r.hsetnx(MONACO_JOBS, jid, job):
                r.hset(MONACO_JOB_STATUS, jid, 'P:%s' % time.time())
                r.rpush(MONACO_JOB_QUEUE, jid)
                return jid
        raise RuntimeError('Failed to generate an unused unique JID')

    def log_progress(self, jid, msg, r=None):
        ''' appends msg to the output for this jid '''
        if not r:
            r = self.r
        with r.pipeline() as pipe:
            pipe.append(MONACO_JOB_OUTPUT_TMPL % jid, msg)
            pipe.expire(MONACO_JOB_OUTPUT_TMPL % jid, 10*60) # 10mins
            pipe.execute()

    def finish_job(self, jid, result, r=None):
        ''' updates the status for the job '''
        if not r:
            r = self.r
        status = 'F%s:%s' % ('+' if result else '-', time.time())
        # F+:123412341.1234 success @ ts time
        # F-:1234121243.123 fail @ ts time
        r.hset(MONACO_JOB_STATUS, jid, status)

    def delete_job(self, jid, r=None):
        ''' This is used to clean up an old JID, NOT as a part of job control '''
        if not r:
            r = self.r
        with r.pipeline() as pipe:
            pipe.hdel(MONACO_JOBS, jid)
            pipe.hdel(MONACO_JOB_STATUS, jid)
            pipe.delete(MONACO_JOB_OUTPUT_TMPL % jid)
            pipe.execute()

class Monaco(object):
    '''
    # Mostly for UI, user help
    self.node_ids_by_hostname = {'hostname': 'node_id', ...}
    self.app_ids_by_service = {'service name': 'app_id', ...}
    self.twem_ids_by_name = {'proxy name': 'twem_id', ...}
    self.hostnames_by_node_id = {'node_id': 'hostname', ...}
    self.service_by_app_id = {'app_id': 'service name', ...}
    self.name_by_twem_id = {'twem_id': 'proxy name', ...}

    # Comprehensive sets of nodes, apps, and twemproxies in this Monaco Cluster
    self.node_ids = [node_id, node_id, ...]
    self.app_ids = [app_id, app_id, ...]
    self.twem_ids = [twem_id, twem_id, ...]

    # Task Queue
    self.job_queue = []
    '''
    def __init__(self, **kwargs):
        '''
        For consistency sake, not expecting any kwargs
        '''
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    def refresh(self, r):
        ''' update object with redis state '''
        try:
            self.node_ids_by_hostname = r.hgetall(MONACO_NIBH)
            self.app_ids_by_service = r.hgetall(MONACO_AIBS)
            self.twem_ids_by_name = r.hgetall(MONACO_TIBN)
            self.hostnames_by_node_id = r.hgetall(MONACO_HBNI)
            self.service_by_app_id = r.hgetall(MONACO_SBAI)
            self.name_by_twem_id = r.hgetall(MONACO_NBTI)

            self.node_ids = list(r.smembers(MONACO_NODES))
            self.app_ids = list(r.smembers(MONACO_APPS))
            self.twem_ids = list(r.smembers(MONACO_TWEMS))
            return True
        except Exception:
            return False

    def next_app_id(self, r):
        ''' return next unused app_id '''
        app_id = int(r.incr(MONACO_NEXT_APP_ID))
        if app_id < MIN_APP_ID or app_id > MAX_APP_ID:
            raise RuntimeError("Couldn't get acceptable app_id")
        if str(app_id) in self.app_ids:
            raise RuntimeError("Already an app at the next chosen app_id")
        return app_id

    def next_twem_id(self, r):
        ''' return next unused twem_id '''
        twem_id = int(r.incr(MONACO_NEXT_TWEM_ID))
        if twem_id % 2:
            # twem_id's are even.
            twem_id = int(r.incr(MONACO_NEXT_TWEM_ID))
        if twem_id < MIN_TWEM_ID or twem_id > MAX_TWEM_ID:
            raise RuntimeError("Couldn't get acceptable twem_id")
        if str(twem_id) in self.twem_ids:
            raise RuntimeError("Already a twemproxy at the next chosen twem_id")
        return twem_id

    def new_app(self, service, app_id, r):
        ''' create servicename/app_id mappings for a new app '''
        # Update redis
        with r.pipeline() as pipe:
            pipe.hset(MONACO_AIBS, service, app_id)
            pipe.hset(MONACO_SBAI, app_id, service)
            pipe.sadd(MONACO_APPS, app_id)
            pipe.execute()
        # Update class vars
        try:
            self.app_ids_by_service[service] = app_id
            self.service_by_app_id[app_id] = service
            self.app_ids.append(app_id)
        except AttributeError:
            # The class is being used un-refreshed, nbd
            pass

    def rename_app(self, app_id, service, r):
        ''' changes the service name of an app '''
        # Update redis
        with r.pipeline() as pipe:
            pipe.hset(MONACO_AIBS, service, app_id)
            pipe.hdel(MONACO_AIBS, self.service_by_app_id[app_id])
            pipe.hset(MONACO_SBAI, app_id, service)
            pipe.execute()
        # Update class vars
        try:
            self.app_ids_by_service[service] = app_id
            del self.app_ids_by_service[self.service_by_app_id[app_id]]
            self.service_by_app_id[app_id] = service
        except AttributeError:
            # The class is being used un-refreshed, nbd
            pass

    def delete_app(self, app_id, r):
        ''' delete app from top-level monaco '''
        # Update redis
        service = r.hget(MONACO_SBAI, app_id)
        with r.pipeline() as pipe:
            pipe.hdel(MONACO_SBAI, app_id)
            pipe.hdel(MONACO_AIBS, service)
            pipe.srem(MONACO_APPS, app_id)
            pipe.execute()
        # Update class vars
        try:
            del self.app_ids_by_service[service]
            del self.service_by_app_id[app_id]
            self.app_ids.remove(app_id)
        except (AttributeError, KeyError):
            # The class is being used un-refreshed, nbd
            pass

    def new_twem(self, twem_id, name, r):
        ''' create twem mappings '''
        # Update redis
        with r.pipeline() as pipe:
            pipe.hset(MONACO_NBTI, twem_id, name)
            pipe.hset(MONACO_TIBN, name, twem_id)
            pipe.sadd(MONACO_TWEMS, twem_id)
            pipe.execute()
        # Update class vars
        try:
            self.twem_ids_by_name[name] = twem_id
            self.names_by_twem_id[twem_id] = name
            self.twem_ids.append(twem_id)
        except AttributeError:
            # The class is being used un-refreshed, nbd
            pass

    def rename_twem(self, twem_id, name, r):
        ''' rename a twem '''
        # Redis update
        with r.pipeline() as pipe:
            pipe.hset(MONACO_TIBN, name, twem_id)
            pipe.hdel(MONACO_TIBN, self.name_by_twem_id[twem_id])
            pipe.hset(MONACO_NBTI, twem_id, name)
            pipe.execute()
        # Update class vars
        try:
            self.twem_ids_by_name[name] = twem_id
            self.names_by_twem_id[twem_id] = name
            self.twem_ids.append(twem_id)
        except AttributeError:
            # The class is being used un-refreshed, nbd
            pass

    def delete_twem(self, twem_id, r):
        ''' delete twem mappings '''
        # Update redis
        with r.pipeline() as pipe:
            pipe.hdel(MONACO_TIBN, self.name_by_twem_id[twem_id])
            pipe.hdel(MONACO_NBTI, twem_id)
            pipe.srem(MONACO_TWEMS, twem_id)
            pipe.execute()
        # Update class vars
        try:
            del self.twem_ids_by_name[self.names_by_twem_id[twem_id]]
            del self.names_by_twem_id[twem_id]
            self.twem_ids.remove(twem_id)
        except (AttributeError, KeyError):
            # The class is being used un-refreshed, nbd
            pass

    def new_node(self, node, r):
        ''' add hostname/id mappings for new node '''
        # Update redis
        with r.pipeline() as pipe:
            pipe.sadd(MONACO_NODES, node.node_id)
            pipe.hset(MONACO_NIBH, node.hostname, node.node_id)
            pipe.hset(MONACO_HBNI, node.node_id, node.hostname)
            pipe.execute()
        # Update class vars
        try:
            self.node_ids_by_hostname[node.hostname] = node.node_id
            self.hostnames_by_node_id[node.node_id] = node.hostname
            self.node_ids.append(node.node_id)
        except AttributeError:
            # The class is being used un-refreshed, nbd
            pass

    def delete_node(self, node, r):
        ''' delete node '''
        # Update redis
        with r.pipeline() as pipe:
            pipe.srem(MONACO_NODES, node.node_id)
            pipe.hdel(MONACO_NIBH, node.hostname)
            pipe.hdel(MONACO_HBNI, node.node_id)
            pipe.execute()
        # Update class vars
        try:
            del self.node_ids_by_hostname[node.hostname]
            del self.hostnames_by_node_id[node.node_id]
            self.node_ids.remove(node.node_id)
        except AttributeError:
            # The class is being used un-refreshed, nbd
            pass

    def node_up(self, node, r):
        ''' backwards compatibility- also, node changes come from top level '''
        if type(node) != MonacoNode:
            node = MonacoNode(node_id=node)
            node.refresh(r)
        return node.status_up(r)

    def node_down(self, node, r):
        ''' compat '''
        if type(node) != MonacoNode:
            node = MonacoNode(node_id=node)
            node.refresh(r)
        return node.status_down(r)

    def node_maintenance(self, node, r):
        ''' compat '''
        if type(node) != MonacoNode:
            node = MonacoNode(node_id=node)
            node.refresh(r)
        return node.status_maintenance(r)

    # TODO: DELETE?
    def next_job(self, r):
        ''' returns the job dict for the next job in the job queue '''
        return pickle.loads(r.lpop(MONACO_JOB_QUEUE))
    def job_count(self, r):
        ''' returns the number of jobs pending '''
        return r.llen(MONACO_JOB_QUEUE)
    def pushback_job(self, job, r):
        ''' queues a job. see master.API_FUNCTIONS '''
        return r.rpush(MONACO_JOB_QUEUE, pickle.dumps(job))


class MonacoNode(object):
    '''
    # Node Info
    self.node_id = int as str type
    self.hostname = 'hostname'
    self.FQDN = fqdn
    self.total_memory = '131072' # string of int for MB
    self.rack = '408'
    self.apps = [app_id, app_id, ...]
    self.twems = [twem_id, twem_id, ...]
    # Monaco Info
    self.status = 'UP' | 'DOWN'
    self.hb_count = 0 # For 3 ping rule of thumb for node down
    '''

    HASH_KEYS = ['hostname', 'FQDN', 'total_memory', 'rack', 'status', 'hb_count']

    def __init__(self, **kwargs):
        '''
        node_id required, can pass other attrs
        '''
        if not 'node_id' in kwargs.keys():
            raise RuntimeError('No node_id provided to MonacoNode constructor')
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    def refresh(self, r):
        ''' refresh this object from redis '''
        for k, v in r.hgetall(NODE_HASH_TMPL % self.node_id).iteritems():
            if k in self.HASH_KEYS:
                setattr(self, k, v)
        self.apps = list(r.smembers(NODE_APPS_TMPL % self.node_id))
        self.twems = list(r.smembers(NODE_TWEMS_TMPL % self.node_id))

    def delete(self, r):
        '''
        Deletes redis structs related to this node
        '''
        with r.pipeline() as pipe:
            pipe.delete(NODE_HASH_TMPL % self.node_id)
            pipe.delete(NODE_APPS_TMPL % self.node_id)
            pipe.execute()

    def write(self, r):
        ''' updates redis to reflect this object '''
        self.write_hash(r)
        self.write_appset(r)
        self.write_twemset(r)

    def write_hash(self, r):
        ''' updates the node info hash '''
        hash_update(NODE_HASH_TMPL % self.node_id, dict([(k, getattr(self, k)) for k in self.HASH_KEYS if hasattr(self, k)]), r)

    def write_appset(self, r):
        ''' updates the set of apps on this node '''
        with r.pipeline() as pipe:
            for app_id in r.smembers(NODE_APPS_TMPL % self.node_id):
                if not app_id in self.apps:
                    pipe.srem(NODE_APPS_TMPL % self.node_id, app_id)
            for app_id in self.apps:
                pipe.sadd(NODE_APPS_TMPL % self.node_id, app_id)
            pipe.execute()

    def write_twemset(self, r):
        ''' updates the set of twems on this node '''
        with r.pipeline() as pipe:
            for twem_id in r.smembers(NODE_TWEMS_TMPL % self.node_id):
                if not twem_id in self.twems:
                    pipe.srem(NODE_TWEMS_TMPL % self.node_id, twem_id)
            for twem_id in self.twems:
                pipe.sadd(NODE_TWEMS_TMPL % self.node_id, twem_id)
            pipe.execute()

    def trigger_twem_listener(self, r):
        ''' adds and removes an invalid twem_id to the twem set for this node in pipeline '''
        with r.pipeline() as pipe:
            pipe.sadd(NODE_TWEMS_TMPL % self.node_id, '-1')
            pipe.srem(NODE_TWEMS_TMPL % self.node_id, '-1')
            pipe.execute()

    def status_up(self, r, force=False):
        '''
        Used for bringing a self back to the 'UP' state, unless in maintenance state.
        If force is set, and the self is in maintenance state, it is returned to UP.
        '''
        self.refresh(r)
        if hasattr(self, 'status') and self.status == 'MAINTENANCE' and not force:
            return False
        self.status = 'UP'
        self.hb_count = 0
        self.write_hash(r)
        return True

    def status_down(self, r):
        '''
        Increments the downcount- on reaching the threshhold, it marks down the self
        If the self was in MAINTENANCE state, noop
        '''
        self.refresh(r)
        if not hasattr(self, 'status'):
            self.status = 'DOWN'
            self.hb_count = 0
            self.write_hash(r)
            return True
        if self.status == 'MAINTENANCE':
            return False
        self.hb_count = int(self.hb_count) + 1
        change = False
        # TODO: config?
        if self.hb_count >= 3:
            change = self.status != 'DOWN'
            self.status = 'DOWN'
        self.write_hash(r)
        return change

    def status_maintenance(self, r):
        '''
        Used for marking a Node as receiving maintenance.
        NOTE: this doesn't offload any of the services on the node- that must be done elsewhere
        This just sets the node's state, so new apps don't get allocated to it.
        '''
        self.refresh(r)
        self.status = 'MAINTENANCE'
        self.hb_count = 0
        self.write_hash(r)
        return True

    def app_info(self, r):
        '''
        returns info on the apps on the node + node utilization in MB
        '''
        masters = []
        slaves = []
        memory = 0
        for app_id in self.apps:
            app = App(app_id=app_id)
            app.refresh(r)
            if not self.node_id in app.nodes.keys():
                raise RuntimeError('App and node disagree')
            if app.nodes[self.node_id] == 'slave':
                slaves.append(app_id)
            elif app.nodes[self.node_id] == 'master':
                masters.append(app_id)
            memory += int(app.maxmemory) // (1024 * 1024)
        return {
            'masters': masters,
            'slaves': slaves,
            'memory': memory,
        }


class App(object):
    '''
    # DB Info
    self.app_id = 1234
    self.shard_size = '8mb' #TODO use bytes (or mb)? standard int format
    self.maxmemory_policy = 'volatile-lru'
    self.persist = True|False
    self.replicas = 3
    self.nodes = {'node1': 'slave', 'node2': 'slave':, 'node3', 'master'}
    # Monaco Info
    self.revision = 1-1000000 (ID for config)
    self.owner = 'user.name'
    self.operator = 'email'
    self.slavelb = True|False
    # Status Info
    self.get_status() --> {
        'nodes': [<node_id>, <node_id>, ...]
        -- for each <node_id> in 'nodes' --
        <node_id>: {
            'revision_<node_id>': config revision on <node_id>,
            'state_<node_id>': ['run', 'reload', 'stop'] ^^,
            'offset_<node_id>': slave-repl-offset for app server on <node_id>,
        }
    }
    '''

    @property
    def port(self):
        ''' permanent workaround ;) '''
        return self.app_id

    @property
    def master(self):
        ''' returns the node_id of the master of this DB '''
        for node_id, role in self.nodes.iteritems():
            if role == 'master':
                return node_id
        return None

    HASH_KEYS = ['name', 'maxmemory', 'maxmemory_policy', 'persist', 'revision', 'replicas', 'owner', 'operator', 'slavelb', 'ypservice']
    ESSENTIAL_KEYS = ['maxmemory', 'maxmemory_policy', 'persist', 'replicas', 'owner', 'operator']
    VALID_STATES = ['run', 'reload', 'stop']

    def __init__(self, **kwargs):
        ''' app_id required, can pass other attrs '''
        if not 'app_id' in kwargs.keys():
            raise RuntimeError('No app_id provided to App constructor')
        for k, v in  kwargs.iteritems():
            setattr(self, k, v)

    def exists(self, r):
        ''' Explains itself.. APPEASE THE PYLINT! APPEASE REBERT!! '''
        return r.exists(APP_HASH_TMPL % self.app_id) and r.exists(APP_CLUSTER_TMPL % self.app_id)

    def refresh(self, r):
        ''' update this object to reflect redis '''
        for k, v in r.hgetall(APP_HASH_TMPL % self.app_id).iteritems():
            if k in self.HASH_KEYS:
                setattr(self, k, v)
        self.nodes = r.hgetall(APP_CLUSTER_TMPL % self.app_id)

    def update_revision(self):
        ''' should be called when the app is updated '''
        try:
            self.revision = int(self.revision) + 1
            if self.revision < 0:
                self.revision = 1
        except Exception:
            self.revision = 1

    def write(self, r):
        ''' update redis to reflect this object '''
        self.write_hash(r)
        self.write_cluster(r)

    def write_hash(self, r):
        '''
        This only updates redis to reflect this object's config hash
        '''
        hash_update(APP_HASH_TMPL % self.app_id, dict([(k, getattr(self, k)) for k in self.HASH_KEYS if hasattr(self, k)]), r)

    def write_cluster(self, r):
        '''
        This only updates redis to reflect this object's node mapping
        '''
        hash_write(APP_CLUSTER_TMPL % self.app_id, self.nodes, r)

    def set_node_revision(self, node_id, revision, r):
        ''' Updates a node's revision in status struct '''
        hash_update(APP_NODEVERSION_TMPL % self.app_id, {'revision_%s' % node_id: revision}, r)

    def set_node_state(self, node_id, state, r):
        ''' Updates a node's state in status struct '''
        assert state in self.VALID_STATES
        hash_update(APP_NODESTATUS_TMPL % self.app_id, {'state_%s' % node_id: state}, r)

    def set_node_offset(self, node_id, offset, r):
        ''' Updates a node's slave_repl_offset in status struct '''
        hash_update(APP_NODEVERSION_TMPL % self.app_id, {'offset_%s' % node_id: offset}, r)

    def set_node_status(self, node_id, r, revision=None, state=None, offset=None):
        ''' Updates a node's status struct with whichever fields are provided '''
        data = {}
        if state:
            self.set_node_state(node_id, state, r)
        if revision:
            data['revision_%s' % node_id] = revision
        if offset:
            data['offset_%s' % node_id] = offset
        hash_update(APP_NODEVERSION_TMPL % self.app_id, data, r)

    def clear_node_status(self, node_id, r):
        ''' Removes node's status fields from this app's status struct '''
        r.hdel(APP_NODESTATUS_TMPL % self.app_id, 'stats_%s' % node_id)
        r.hdel(APP_NODEVERSION_TMPL % self.app_id, *['revision_%s' % node_id, 'offset_%s' % node_id])

    def get_status(self, r):
        ''' Retrieves the node status for all in the cluster '''
        status = {'nodes': self.nodes}
        for node_id in self.nodes:
            status[node_id] = self.get_node_status(node_id, r)
        return status

    def get_node_status(self, node_id, r):
        ''' Retrieves a node's status '''
        status = {
            'revision': 0,
            'state': None,
            'offset': 0,
        }
        state = r.hgetall(APP_NODESTATUS_TMPL % self.app_id)
        version = r.hgetall(APP_NODEVERSION_TMPL % self.app_id)

        if 'revision_%s' % node_id in version:
            status['revision'] = int(version['revision_%s' % node_id])
        if 'offset_%s' % node_id in version:
            status['offset'] = version['offset_%s' % node_id]
        if 'state_%s' % node_id in state:
            status['state'] = state['state_%s' % node_id]
        return status

    def wait_status(self, r, status='run', revision=None, timeout=30, sleep_interval=0.01):
        '''
        This blocks for <time> seconds, or until all nodes in this app report:
        The status <status>, and if specified, match <revision>
        '''
        end_time = time.time() + timeout
        while time.time() < end_time:
            for node_id, _ in self.nodes.iteritems():
                stat = self.get_node_status(node_id, r)
                if stat['state'] != status:
                    break
                if revision and stat['revision'] != int(revision):
                    break
            else:
                return True
            time.sleep(sleep_interval)
        # Timeout
        return False

    def delete(self, r):
        ''' Does what it says on the label.. be careful '''
        with r.pipeline() as pipe:
            pipe.delete(APP_HASH_TMPL % self.app_id)
            pipe.delete(APP_CLUSTER_TMPL % self.app_id)
            pipe.delete(APP_NODESTATUS_TMPL % self.app_id)
            pipe.delete(APP_NODEVERSION_TMPL % self.app_id)
            pipe.execute()

    def best_slave(self, r):
        '''
        This method is named horribly- but I don't have HR training till Weds.
        Expectes self to be refreshed, returns the node_id of the most replicated slave
        Raises RuntimeError in any other case
        '''
        if int(self.replicas) == 1:
            raise RuntimeError('Called best_slave on a non-replicated App')
        max_replication = 0
        best_slave = None
        for node_id, role in self.nodes.iteritems():
            if role == 'master':
                continue
            node = MonacoNode(node_id=node_id)
            node.refresh(r)
            try:
                cli = redis.StrictRedis(node.hostname, self.port, socket_connect_timeout=1, socket_timeout=1)
                info = cli.info()
                if info['slave_repl_offset'] > max_replication:
                    best_slave = node_id
                    max_replication = info['slave_repl_offset']
            except redis.RedisError:
                # clearly not the most up-to-date
                continue
        if best_slave:
            return best_slave
        else:
            raise RuntimeError('Unable to find max-replicated slave!')

    def translate(self, cmd):
        '''
        Takes a `cmd` and translates it to the obfuscated name for this App
        This particular algorithm is shit but gets the job done
        '''
        newcmd = ''
        for char in cmd.upper():
            newcmd += chr((ord(char) * int(self.app_id)) % 26 + 65)
        return newcmd

    def get_local_admin_cli(self):
        '''
        Returns a StrictRedis client to the local instance of this app
        Also remaps config_set, config_get, config_rewrite, and shutdown
        '''
        # The easy part
        cli = redis.StrictRedis(port=self.app_id, socket_connect_timeout=1, socket_timeout=1)
        # Translate renamed functions
        conf = self.translate('config')

        confget = '%s GET' % conf
        cli.response_callbacks[confget] = redis.client.parse_config_get
        def config_get(pattern="*"):
            ''' from github.com/andymcurdy/redis-py '''
            return cli.execute_command(confget, pattern)
        cli.config_get = config_get

        confset = '%s SET' % conf
        cli.response_callbacks[confset] = redis.client.bool_ok
        def config_set(name, value):
            ''' from github.com/andymcurdy/redis-py '''
            return cli.execute_command(confset, name, value)
        cli.config_set = config_set

        confrewrite = '%s REWRITE' % conf
        cli.response_callbacks[confrewrite] = redis.client.bool_ok
        def config_rewrite():
            ''' hey, I wrote this one ;) '''
            return cli.execute_command(confrewrite)
        cli.config_rewrite = config_rewrite

        shutdown_cmd = self.translate('shutdown')
        cli.response_callbacks[shutdown_cmd] = redis.client.bool_ok
        def shutdown():
            ''' from github.com/andymcurdy/redis-py '''
            try:
                cli.execute_command(shutdown_cmd)
            except redis.ConnectionError:
                # Expected here
                return
            raise redis.RedisError('SHUTDOWN seems to have failed')
        cli.shutdown = shutdown

        return cli

    def get_master_connection(self, r):
        ''' Returns a StrictRedis client to this app's master (direct, no LB) '''
        for k, v in self.nodes.iteritems():
            if v == 'master':
                node_id = k
        node = MonacoNode(node_id=node_id)
        node.refresh(r)
        return redis.StrictRedis(host=node.hostname, port=self.app_id, socket_connect_timeout=1, socket_timeout=1)

class MonacoTwem(object):
    '''
    Interface for twemproxy configurations
    # Monaco Info (needs config formatting, or not relevant)
    self.name = name for proxy
    self.owner = uname of owner
    self.operator = uname of operator group
    self.port = port that this proxy will listen on
    self.servers = [app_id, app_id, ...] servers that this proxies
    self.nodes = [node_id, node_id, ...] nodes that host this proxy
    self.extservers = ['serverhost:serverport', ...] out-of-cluster servers to also proxy
    # Twem Info (in config format)
    self.listen = managed string for listen IP:PORT
    self.hash = 'one_at_a_time', 'md5', 'crc16', 'crc32', 'crc32a', 'fnv1_64', 'fnv1a_64', 'fnv1_32', 'fnv1a_32', 'hsieh', 'murmur', 'jenkins'
    self.hash_tag = None, or two char string (read up, ways to make your shit shard your way)
    self.distribution = 'ketama', 'modula', 'random' # always random for single DB proxies
    self.timeout = msec
    self.server_connections = 1-5 # probably wont expose, and keep at 1
    '''

    HASH_KEYS = ['name', 'owner', 'operator', 'hash', 'hash_tag', 'distribution', 'timeout', 'server_connections']
    ESSENTIAL_KEYS = ['name', 'owner', 'operator', 'hash', 'distribution']

    def __init__(self, **kwargs):
        '''
        twem_id required, can pass other attrs
        '''
        if not 'twem_id' in kwargs.keys():
            raise RuntimeError('No twem_id provided to MonacoTwem constructor')
        for k, v in  kwargs.iteritems():
            setattr(self, k, v)

    def refresh(self, r):
        ''' update this object to reflect redis '''
        for key, val in r.hgetall(TWEM_HASH_TMPL % self.twem_id).iteritems():
            if key in self.HASH_KEYS:
                setattr(self, key, val)
        self.servers = list(r.smembers(TWEM_CLUSTER_TMPL % self.twem_id))
        self.nodes = list(r.smembers(TWEM_NODES_TMPL % self.twem_id))
        self.extservers = list(r.smembers(TWEM_EXTSRV_TMPL % self.twem_id))

    def write(self, r):
        ''' update redis to reflect this object '''
        hash_update(TWEM_HASH_TMPL % self.twem_id, dict([(k, getattr(self, k)) for k in self.HASH_KEYS if hasattr(self, k)]), r)
        with r.pipeline() as pipe:
            # update self.servers
            for server in r.smembers(TWEM_CLUSTER_TMPL % self.twem_id):
                if not server in self.servers:
                    pipe.srem(TWEM_CLUSTER_TMPL % self.twem_id, server)
            for server in self.servers:
                pipe.sadd(TWEM_CLUSTER_TMPL % self.twem_id, server)
            # update self.nodes
            for node_id in r.smembers(TWEM_NODES_TMPL % self.twem_id):
                if not node_id in self.nodes:
                    pipe.srem(TWEM_NODES_TMPL % self.twem_id, node_id)
            for node_id in self.nodes:
                pipe.sadd(TWEM_NODES_TMPL % self.twem_id, node_id)
            # update self.extservers
            for server in r.smembers(TWEM_EXTSRV_TMPL % self.twem_id):
                if not server in self.extservers:
                    pipe.srem(TWEM_EXTSRV_TMPL % self.twem_id, server)
            for server in self.extservers:
                pipe.sadd(TWEM_EXTSRV_TMPL % self.twem_id, server)

            pipe.execute()

    def delete(self, r):
        ''' delete this twemproxy from redis '''
        with r.pipeline() as pipe:
            pipe.delete(TWEM_HASH_TMPL % self.twem_id)
            pipe.delete(TWEM_CLUSTER_TMPL % self.twem_id)
            pipe.delete(TWEM_NODES_TMPL % self.twem_id)
            pipe.delete(TWEM_EXTSRV_TMPL % self.twem_id)
            pipe.execute()

    @property
    def listen(self):
        ''' read-only '''
        return '0.0.0.0:%s' % self.port

    @property
    def port(self):
        ''' mapped to twem_id's in this case '''
        port = int(self.twem_id)
        if port % 2:
            raise RuntimeError('Twem ports are always even!')
        return port

    @property
    def stat_port(self):
        ''' always mapped to 1 + self.port '''
        return self.port + 1

    @property
    def auto_eject_hosts(self):
        ''' Because we only want to eject hosts when we're balancing across slaves '''
        return len(self.servers) == 1

    @property
    def distribution(self):
        ''' Because we want to force random if using a single DB '''
        if len(self.servers) == 1:
            return 'random'
        return self._distribution

    @distribution.setter
    def distribution(self, value):
        ''' setter '''
        self._distribution = value
