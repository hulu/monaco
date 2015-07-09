#!/usr/bin/python
'''
This defines system tests that can be run against a running installation of Monaco
to verify the consistency/sanity/status of it's internals
'''
import redis
import schema
import re
import logging
import StringIO

# tests
def check_all_apps_in_apps_list(testlogger, r):
    '''
    Check if the monaco-apps set represents the set of monaco app hashes in r
    '''
    m = schema.Monaco()
    m.refresh(r)
    getid = re.compile(r'-([0-9]*)$')
    noextra = True
    # mix the template with a wildcard to match all
    for app in r.keys(schema.APP_HASH_TMPL % '*'):
        match = getid.search(app)
        if not match:
            continue
        app = match.group(1)
        if app in m.app_ids:
            m.app_ids.remove(app)
        else:
            testlogger.warn('Extra app found at key: %s' % app)
            noextra = False
    for app in m.app_ids:
        testlogger.error('App %s found in top-level app list, but no corresponding struct found' % app)
    return len(m.app_ids) == 0 and noextra
        
def check_all_nodes_in_nodes_list(testlogger, r):
    '''
    Same as above, but for nodes
    '''
    m = schema.Monaco()
    m.refresh(r)
    getid = re.compile(r'-([0-9]*)$')
    noextra = True
    for node in r.keys(schema.NODE_HASH_TMPL % '*'):
        match = getid.search(node)
        if not match:
            continue
        node = match.group(1)
        if node in m.node_ids:
            m.node_ids.remove(node)
        else:
            testlogger.warn('Extra node struct found at key: %s' % node)
            noextra = False
    for node in m.node_ids:
        testlogger.error('Node %s found in top-level node list, but no corresponding struct found' % node)
    return len(m.node_ids) == 0 and noextra

def check_apps_and_clusters(testlogger, r):
    '''
    Check that apps:clusters is 1:1
    '''
    result = True
    app_hashes = [key for key in r.keys(schema.APP_HASH_TMPL % '*') if not (key.endswith('cluster') or key.endswith('status') or key.endswith('version'))]
    app_clusters = r.keys(schema.APP_CLUSTER_TMPL % '*')
    m = schema.Monaco()
    m.refresh(r)
    for app_id in m.app_ids:
        hashkey = schema.APP_HASH_TMPL % app_id
        clusterkey = schema.APP_CLUSTER_TMPL % app_id
        if hashkey in app_hashes:
            app_hashes.remove(hashkey)
        else:
            testlogger.error("App %s doesn't have a corresponding app hash", app_id)
            result = False
        if clusterkey in app_clusters:
            app_clusters.remove(clusterkey)
        else:
            testlogger.error("App %s doesn't have a corresponding cluster hash", app_id)
            result = False
    if len(app_hashes) != 0:
        testlogger.warn('Extra app hashes: %s', app_hashes)
        result = False
    if len(app_clusters) != 0:
        testlogger.warn('Extra cluster hashes: %s', app_clusters)
        result = False
    return result

def check_clusters_vs_node_apps(testlogger, r):
    '''
    Check that clusters match node-app sets
    '''
    result = True
    m = schema.Monaco()
    m.refresh(r)
    for app_id in m.app_ids:
        app = schema.App(app_id=app_id)
        app.refresh(r)
        for node_id, _ in app.nodes.iteritems():
            if not app_id in r.smembers(schema.NODE_APPS_TMPL % node_id):
                testlogger.error("App %s has node %s in it's cluster, but the node doesn't have it in it's app-set", app_id, node_id)
                result = False
    for node_id in m.node_ids:
        node = schema.MonacoNode(node_id=node_id)
        node.refresh(r)
        for app_id in node.apps:
            if not node_id in r.hkeys(schema.APP_CLUSTER_TMPL % app_id):
                testlogger.error("Node %s has app %s in its app-set, but the corresponding app doesn't have the node in it's cluster", node_id, app_id)
                result = False
    return result

def validate_app_invariences(testlogger, r):
    '''
    Ensure that the user specified replica count is maintained
    '''
    result = True
    m = schema.Monaco()
    m.refresh(r)
    for app_id in m.app_ids:
        app = schema.App(app_id=app_id)
        app.refresh(r)
        if int(app.replicas) != len(app.nodes):
            testlogger.error("App %s doesn't have the desired replica count", app_id)
            result = False
    return result

def check_job_queue_sanity(testlogger, r, limit=5):
    '''
    Ensure that there are less than limit jobs pending in queue
    '''
    return r.llen(schema.MONACO_JOB_QUEUE) <= limit

def check_allocated_nodes_online(testlogger, r):
    '''
    Ensure that any node allocated to hold a redis instance for an app is online
    NOTE: This only verifies against the reported 'status'.
    '''
    result = True
    m = schema.Monaco()
    m.refresh(r)
    for app_id in m.app_ids:
        for node_id, _ in r.hgetall(schema.APP_CLUSTER_TMPL % app_id).iteritems():
            if r.hget(schema.NODE_HASH_TMPL % node_id, 'status') != 'UP':
                testlogger.error("App %s has node %s allocated to host an instance, despite the node being marked DOWN." % (app_id, node_id))
                result = False
    return result

def check_all_apps_have_all_hash_fields(testlogger, r):
    '''
    Check that all the app hashes have the expected keys
    '''
    KEYS = ['maxmemory', 'maxmemory_policy', 'persist', 'replicas', 'owner', 'operator']
    result = True
    for app_id in r.smembers(schema.MONACO_APPS):
        hashkeys = r.hkeys(schema.APP_HASH_TMPL % app_id)
        for key in KEYS:
            if not key in hashkeys:
                if key == 'maxmemory' and 'shard_size' in hashkeys:
                    testlogger.warn('App %s is using old schema shard_size' % app_id)
                else:
                    testlogger.error('App %s does not have hashkey %s present in the conf struct' % (app_id, key))
                    result = False
    return result

def check_cluster_sanity(testlogger, r):
    '''
    Check that each cluster has only 1 master
    '''
    result = True
    m = schema.Monaco()
    m.refresh(r)

    for app_id in m.app_ids:
        cluster = r.hgetall(schema.APP_CLUSTER_TMPL % app_id)
        master = [k for k,v in cluster.iteritems() if v == 'master']
        if len(master) != 1:
            testlogger.error('App %s has %s masters!' % (app_id, len(master)))
            result = False
        invalid = [k for k,v in cluster.iteritems() if not v in ['master', 'slave', 'slave-write']]
        if len(invalid) > 0:
            testlogger.error('App %s has an invalid cluster specification: %s', app_id, cluster)
            result = False
    return result

def run_system_checks(db_host, db_port=6379):
    '''
    runs all tests above against a Monaco DB
    returns (True, '') or (False, 'reason')
    '''
    logger = logging.getLogger('basic_logger')
    logger.setLevel(logging.DEBUG)
    log_capture = StringIO.StringIO()
    ch = logging.StreamHandler(log_capture)
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    r = redis.StrictRedis(host=db_host, port=db_port, socket_timeout=10, socket_connect_timeout=5)

    TESTS = [
        check_all_apps_in_apps_list,
        check_all_nodes_in_nodes_list,
        check_apps_and_clusters,
        check_clusters_vs_node_apps,
        validate_app_invariences,
        check_job_queue_sanity,
        check_allocated_nodes_online,
        check_all_apps_have_all_hash_fields,
        check_cluster_sanity,
    ]
    db_result = True
    for test in TESTS:
        try:
            db_result = db_result and test(logger, r)
        except redis.RedisError, exc:
            db_result = False
            logger.warn('Test %s didnt run properly due to redis connection failure' % repr(test))
            del(r)
            r = redis.StrictRedis(host=db_host, port=db_port, socket_timeout=10, socket_connect_timeout=5)
        except Exception, exc:
            db_result = False
            logger.exception(exc)
    output = log_capture.getvalue()
    log_capture.close()
    return (db_result, output)
