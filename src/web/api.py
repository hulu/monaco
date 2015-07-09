#!/usr/bin/python
# coding=utf-8
'''
Provides Monaco's API 
'''
from __future__ import absolute_import, print_function, division

from redis import StrictRedis
from flask import request, jsonify, abort, make_response
import re
import time

import schema
from web import app, rediscli, monaco_admin, list_apps_by_owner, list_apps_by_operator, list_apps, \
                list_twems, list_twems_by_owner, list_twems_by_operator, monacotests

# API Methods
@app.route('/api/nodes', methods=['GET', 'HEAD'])
def list_nodes_api():
    '''
    This provides a RESTful endpoint for listing MonacoNodes
    HEAD: Returns {'node_ids': [node_id, node_id]}, which only queries master mgmt db
    GET: Returns HEAD + {'<node_id>': {NODE INFO DICT}}, which queries redis servers on each node
    '''
    data = {}
    r = rediscli()
    monaco = schema.Monaco()
    monaco.refresh(r)
    data['node_ids'] = monaco.node_ids

    for node_id in data['node_ids']:
        data[node_id] = {}
        data[node_id]['hostname'] = monaco.hostnames_by_node_id[node_id]
        if request.method == 'HEAD':
            continue
        try:
            rtemp = StrictRedis(host=data[node_id]['hostname'], port=app.config['MGMT_PORT'], socket_connect_timeout=1, socket_timeout=0.5)
            info = rtemp.info()
            data[node_id]['role'] = info['role']
            if data[node_id]['role'] == 'slave':
                data[node_id]['role_details'] = {'master': info['master_host']}
            else:
                data[node_id]['role_details'] = {}
                data[node_id]['role_details']['connected_slaves'] = info['connected_slaves']
                for idx in xrange(int(info['connected_slaves'])):
                    data[node_id]['role_details']['slave%d' % idx] = info['slave%d' % idx]
            node = schema.MonacoNode(node_id=node_id)
            node.refresh(r)
            data[node_id]['mem_usage'] = '%sM' % node.app_info(r)['memory']
            data[node_id]['up'] = True
        except Exception:
            data[node_id]['up'] = False
            data[node_id]['role'] = None
            data[node_id]['role_details'] = None
            data[node_id]['mem_usage'] = None
        data[node_id]['net_usage'] = None
    return jsonify(data)


@monaco_admin
@app.route('/api/nodes', methods=['POST'])
def new_node_api():
    '''
    Create new nodes by POST'ing info to this endpoint.
    Get a listing of nodes
    '''
    r = rediscli()
    monaco = schema.Monaco()
    monaco.refresh(r)

    if request.method == 'POST':
        if str(request.form['node_id']) in monaco.node_ids:
            abort(400)
        newnode = schema.MonacoNode(node_id=request.form['node_id'])
        newnode.apps = []
        newnode.twems = []
        newnode.hostname = request.form['hostname']
        newnode.FQDN = request.form['FQDN']
        newnode.total_memory = request.form['total_memory']
        newnode.rack = request.form['rack']
        newnode.write(r)
        monaco.new_node(newnode, r)
        return 'OK'


@monaco_admin
@app.route('/api/node/<int:node_id>', methods=['GET', 'POST', 'DELETE'])
def node_api(node_id):
    '''
    This provides a RESTful endpoint for MonacoNode management
    GET- get MonacoNode info
    POST- with updated info
    DELETE- an existing node to remove from distribution
    '''
    r = rediscli()
    monaco = schema.Monaco()
    monaco.refresh(r)

    if request.method == 'GET':
        if not str(node_id) in monaco.node_ids:
            abort(404)
        node = schema.MonacoNode(node_id=str(node_id))
        node.refresh(r)
        data = dict(node.__dict__.items() + node.app_info(r).items())
        return jsonify(data)

    if request.method == 'POST':
        if not str(node_id) in monaco.node_ids:
            abort(404)
        node = schema.MonacoNode(node_id=node_id)
        node.refresh(r)
        node.total_memory = request.form['total_memory']
        node.rack = request.form['rack']
        node.write(r)
        return 'OK'

    if request.method == 'DELETE':
        if str(node_id) in monaco.node_ids:
            abort(400)
        nodetokill = schema.MonacoNode(node_id=node_id)
        if len(nodetokill.apps) != 0 or nodetokill.status != 'MAINTENANCE':
            abort(400)
        monaco.delete_node(nodetokill, r)
        nodetokill.delete(r)
        return 'OK'

@app.route('/api/proxies', methods=['GET'])
def list_proxies_api():
    ''' lists twems'''
    r = rediscli()
    monaco = schema.Monaco()
    monaco.refresh(r)

    if 'owner' in request.args:
        twems = list_twems_by_owner(request['owner'])
    elif 'operator' in request.args:
        twems = list_twems_by_operator(set(request['operator']))
    else:
        twems = list_twems()

    twem_data = {}
    for twem_id in twems:
        try:
            twem = schema.MonacoTwem(twem_id=twem_id)
            twem.refresh(r)
            twem_data[twem_id] = {
                'name': twem.name,
                'lb': 'tcp://%s:%s' % (app.config['MONACO_DB'], twem_id),
                'servers': twem.servers,
            }
        except Exception:
            twem_data[twem_id] = {'service': monaco.name_by_twem_id[twem_id]}

    return jsonify(twem_data)

@app.route('/api/proxy', methods=['POST'])
def new_proxy_api():
    ''' for creating new proxies '''
    r = rediscli()
    job_queue = schema.MonacoJobQueue(r)
    monaco = schema.Monaco()
    monaco.refresh(r)
    if request.form['name'] in monaco.twem_ids_by_name:
        abort(400)
    job = {
        'command': 'new_proxy',
    }
    for k in schema.MonacoTwem.HASH_KEYS:
        if k in request.form:
            job[k] = request.form[k]
    job['servers'] = []
    for app_id in request.values.getlist('servers'):
        if app_id in monaco.app_ids:
            job['servers'].append(app_id)
        else:
            abort(400)
    job['extservers'] = []
    ipport = re.compile('^([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}):([0-9]{4,5})$')
    serverport = re.compile('^([a-zA-Z0-9_-]*):([0-9]{4,5})$')
    for extserver in request.values.getlist('extservers'):
        if ipport.match(extserver):
            job['extservers'].append(extserver)
        elif serverport.match(extserver):
            job['extservers'].append(extserver)
        else:
            abort(400)
    job['hash'] = 'murmur'
    jid = job_queue.pushback_job(job)
    return jsonify(jid=jid)

@app.route('/api/proxy/<int:twem_id>', methods=['GET', 'HEAD', 'POST', 'DELETE'])
def proxy_api(twem_id):
    '''
    Twemproxy API:
    GET:
        200 - gets info about twem in json format
        404 - twem_id does not exist
        403 - you aint allowed
    HEAD:
        200 - twem_id exists
        404 - twem_id does not exist
    POST:
        200 - sent update command to master
        400 - twem_id does not exist
        403 - you aint allowed
    DELETE:
        200 - sent delete command to master
        404 - twem_id does not exist
        403 - you aint allowed
    '''
    r = rediscli()
    job_queue = schema.MonacoJobQueue(r)
    monaco = schema.Monaco()
    monaco.refresh(r)
    twem_id = str(twem_id)

    if request.method == 'HEAD':
        if not twem_id in monaco.twem_ids:
            abort(404)
        return 'OK'

    if request.method == 'GET':
        if not twem_id in monaco.twem_ids:
            abort(404)
        twem = schema.MonacoTwem(twem_id=twem_id)
        twem.refresh(r)
        data = {}
        for key in schema.MonacoTwem.HASH_KEYS:
            if hasattr(twem, key):
                data[key] = getattr(twem, key)
        data['servers'] = twem.servers
        data['extservers'] = twem.extservers
        return jsonify(data)

    if request.method == 'POST':
        if twem_id in monaco.twem_ids:
            twem = schema.MonacoTwem(twem_id=twem_id)
            twem.refresh(r)
            job = {
                'command': 'update_proxy',
                'twem_id': twem_id,
            }
            for key in schema.MonacoTwem.HASH_KEYS:
                if key in request.form:
                    job[key] = request.form[key]
            if 'servers' in request.form:
                job['servers'] = []
                for app_id in request.values.getlist('servers'):
                    if app_id in monaco.app_ids:
                        job['servers'].append(app_id)
                    else:
                        abort(400)
            if 'extservers' in request.form:
                job['extservers'] = []
                ipport = re.compile('^([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}):([0-9]{4,5})$')
                serverport = re.compile('^([a-zA-Z0-9-_]*):([0-9]{4,5})$')
                for extserver in request.values.getlist('extservers'):
                    if ipport.match(extserver):
                        job['extservers'].append(extserver)
                    elif serverport.match(extserver):
                        job['extservers'].append(extserver)
                    else:
                        abort(400)

            jid = job_queue.pushback_job(job)
            return jsonify(jid=jid)
        else:
            # can't create. use POST:/api/proxy
            abort(400)

    if request.method == 'DELETE':
        if not twem_id in monaco.twem_ids:
            abort(404)
        twem = schema.MonacoTwem(twem_id=twem_id)
        twem.refresh(r)
        job = {
            'command': 'delete_proxy',
            'twem_id': twem_id,
        }
        jid = job_queue.pushback_job(job)
        return jsonify(jid=jid)

    return 'OK'

@app.route('/api/proxy/<int:twem_id>/stats', methods=['GET'])
def proxy_stats(twem_id):
    '''
    Returns live aggregates for a given proxy
    '''
    r = rediscli()
    monaco = schema.Monaco()
    monaco.refresh(r)

    if not str(twem_id) in monaco.twem_ids:
        abort(404)
    twem = schema.MonacoTwem(twem_id=twem_id)
    twem.refresh(r)

    aggregate_rps = 0
    aggregate_connections = 0

    if len(twem.servers) == 1:
        dbapp = schema.App(app_id=twem.servers[0])
        dbapp.refresh(r)
        for node_id, _ in dbapp.nodes.iteritems():
            appcli = StrictRedis(monaco.hostnames_by_node_id[node_id], dbapp.port)
            info = appcli.info()
            if 'instantaneous_ops_per_sec' in info:
                aggregate_rps += info['instantaneous_ops_per_sec']
            if 'connected_clients' in info:
                aggregate_connections += info['connected_clients']
    else:
        for app_id in twem.servers:
            dbapp = schema.App(app_id=app_id)
            dbapp.refresh(r)
            appcli = dbapp.get_master_connection(r)
            info = appcli.info()
            if 'instantaneous_ops_per_sec' in info:
                aggregate_rps += info['instantaneous_ops_per_sec']
            if 'connected_clients' in info:
                aggregate_connections += info['connected_clients']

    return jsonify({'total_rps': aggregate_rps, 'total_connections': aggregate_connections})

@app.route('/api/apps', methods=['GET'])
def list_app_api():
    ''' lists apps'''
    r = rediscli()
    monaco = schema.Monaco()
    monaco.refresh(r)

    if 'owner' in request.args:
        apps = list_apps_by_owner(request['owner'])
    elif 'operator' in request.args:
        apps = list_apps_by_operator(set(request['operator']))
    else:
        apps = list_apps()

    app_data = {}
    for app_id in apps:
        try:
            dbapp = schema.App(app_id=app_id)
            dbapp.refresh(r)
            masternode = schema.MonacoNode(node_id=dbapp.master)
            masternode.refresh(r)
            mastercli = dbapp.get_master_connection(r)
            info = mastercli.info()
            used = float(info['used_memory']) / (1024 * 1024)
            total = int(dbapp.maxmemory) // (1024 * 1024)
            percent = round((100 * used) / total, 2)
            used = round(used, 2)
            app_data[app_id] = {
                'service': dbapp.name,
                'exposure': 'tcp://%s:%s' % (masternode.FQDN, dbapp.port),
                'memory_used': used,
                'memory_total': total,
                'memory_percent': percent,
                'connected_clients': info['connected_clients'],
                'rps': info['instantaneous_ops_per_sec'],
            }
        except Exception:
            app_data[app_id] = {'service': monaco.service_by_app_id[app_id]}

    return jsonify(app_data)

@app.route('/api/master_latency', methods=['GET'])
def master_latency():
    '''
    This endpoint sends a ping job to the master, returning runtime info/liveness
    If the request is a POST, this will also push the latency to graphite
    '''
    r = rediscli()
    wait = 60 # 60s of waiting for job_queue to empty
    job_queue = schema.MonacoJobQueue(r)
    job = {'command': 'ping'}
    while not job_queue.empty:
        time.sleep(1)
        wait -= 1
        if wait == 0:
            return make_response(('Job queue not empty', 500))
    jid = job_queue.pushback_job(job)
    start_time = time.time()
    if not jid:
        # couldn't get job in queue?
        return make_response(('Failed to add job', 500))
    status = job_queue.status(jid, r)
    wait = 60 # 60s of waiting for master to ping
    while status['status'] != 'finished':
        if time.time() > start_time + wait:
            return make_response(('Timeout waiting for ping', 500))
        time.sleep(0.1)
        status = job_queue.status(jid, r)
    end_time = time.time()
    job_queue.delete_job(jid, r)
    return jsonify({'start': start_time, 'computed_duration': end_time - start_time, 'timestamp': status['timestamp'], 'output': status['output']})

@app.route('/api/app', methods=['POST'])
def new_app_api():
    ''' for creating new apps '''
    r = rediscli()
    job_queue = schema.MonacoJobQueue(r)
    monaco = schema.Monaco()
    monaco.refresh(r)
    if request.form['name'] in monaco.app_ids_by_service:
        abort(400)
    job = {
        'command': 'new',
    }
    for k in schema.App.HASH_KEYS:
        if k in request.form:
            job[k] = request.form[k]
    if not 'persist' in request.form:
        job['persist'] = False
    else:
        job['persist'] = True
    if not 'slavelb' in request.form:
        job['slavelb'] = False
    else:
        job['slavelb'] = True
    jid = job_queue.pushback_job(job)
    if not jid:
        # just retry in app
        abort(503)
    return jsonify(jid=jid)

@app.route('/api/app/<int:app_id>/redis', methods=['GET', 'PUT', 'DELETE', 'POST'])
def app_redis_api(app_id):
    '''
    Simple REST api to redis DBs.
    GET, PUT, DELETE are key operations, and POST allows for any command

    method = 'GET': ../redis?key=key
        return r.get('key')
    method = 'PUT': ../redis?key=key&val=val
        return r.set('key', 'val')
    method = 'DELETE': ../redis?key=key
        return r.delete('key')
    method = 'POST': ../redis?cmd=hset&args=key,hashkey,hashval
        Request args
        return getattr(r,cmd)(*args.split(','))
        aka    r.hset(key, hashkey, hashval)
    '''
    r = rediscli()
    monaco = schema.Monaco()
    monaco.refresh(r)
    app_id = str(app_id)
    if not app_id in monaco.app_ids:
        abort(404)
    userapp = schema.App(app_id=app_id)
    userapp.refresh(r)

    master_host = None
    for node_id, role in userapp.nodes.iteritems():
        if role == 'master':
            master_host = monaco.hostnames_by_node_id[node_id]
            break
    assert master_host != None

    r = StrictRedis(master_host, userapp.port)

    if request.method == 'GET':
        if 'key' not in request.args:
            abort(400)
        return r.get(request.args['key'])
    if request.method == 'PUT':
        if 'key' not in request.args or 'val' not in request.args:
            abort(400)
        return r.set(request.args['key'], request.args['val'])
    if request.method == 'DELETE':
        if 'key' not in request.args:
            abort(400)
        return r.delete(request.args['key'])
    if request.method == 'POST':
        if 'cmd' not in request.args or not hasattr(r, request.args['cmd']):
            abort(400)
        if 'args' in request.args:
            args = request.args['args'].split(',')
        else:
            args = []
        return getattr(r, request.args['cmd'])(*args)
    abort(400)

@app.route('/api/app/<int:app_id>', methods=['GET', 'HEAD', 'POST', 'DELETE'])
def app_api(app_id):
    '''
    App API:
    GET:
        200 - gets info about app in json format
        404 - app_id does not exist
        403 - you aint allowed
    HEAD:
        200 - app_id exists
        404 - app_id does not exist
    POST:
        200 - sent update command to master
        400 - app_id does not exist
        403 - you aint allowed
    DELETE:
        200 - sent delete command to master
        404 - app_id does not exist
        403 - you aint allowed
    '''
    r = rediscli()
    job_queue = schema.MonacoJobQueue(r)
    monaco = schema.Monaco()
    monaco.refresh(r)
    app_id = str(app_id)

    if request.method == 'HEAD':
        if not app_id in monaco.app_ids:
            abort(404)
        return 'OK'

    if request.method == 'GET':
        if not app_id in monaco.app_ids:
            abort(404)
        dbapp = schema.App(app_id=app_id)
        dbapp.refresh(r)

        app_info = {
            'app_id': dbapp.app_id,
            'port': dbapp.port,
            'nodes': [],
            'unused_nodes': [],
        }
        for k in dbapp.HASH_KEYS:
            if hasattr(dbapp, k):
                app_info[k] = getattr(dbapp, k)
        for node_id, role in dbapp.nodes.iteritems():
            node = schema.MonacoNode(node_id=node_id)
            node.refresh(r)
            app_info['nodes'].append({'host': node.hostname, 'node_id': node_id, 'role': role})
        app_info['unused_nodes'] = [node_id for node_id in monaco.node_ids if not node_id in dbapp.nodes]
        return jsonify(app_info)

    if request.method == 'POST':
        if app_id in monaco.app_ids:
            dbapp = schema.App(app_id=app_id)
            dbapp.refresh(r)

            if request.form['name'] != monaco.service_by_app_id[app_id]:
                monaco.rename_app(app_id, request.form['name'], r)

            job = {
                'command': 'update',
                'app_id': app_id,
            }
            for k in schema.App.HASH_KEYS:
                if k in request.form:
                    job[k] = request.form[k]
            if 'persist' in job:
                job['persist'] = True
            else:
                job['persist'] = False
            if 'slavelb' in job:
                job['slavelb'] = True
            else:
                job['slavelb'] = False
            jid = job_queue.pushback_job(job)
            return jsonify(jid=jid)
        else:
            # can't create with an app_id pre-specified.
            abort(400)

    if request.method == 'DELETE':
        if not app_id in monaco.app_ids:
            abort(404)
        dbapp = schema.App(app_id=app_id)
        dbapp.refresh(r)
        job = {
            'command': 'delete',
            'app_id': app_id,
        }
        jid = job_queue.pushback_job(job)
        return jsonify(jid=jid)

@app.route('/api/app/<int:app_id>/stats')
def app_stats(app_id):
    ''' Returns the redis info in json form '''
    try:
        r = rediscli()
        dbapp = schema.App(app_id=app_id)
        dbapp.refresh(r)
        r = dbapp.get_master_connection(r)
        return jsonify(r.info())
    except Exception:
        # spare my email from the hounds of AJAX
        abort(500)

@app.route('/api/jobs', methods=['GET'])
def jobs_api():
    ''' returns top-level info about jobs in json format '''
    r = rediscli()
    job_queue = schema.MonacoJobQueue(r)
    return jsonify({'pending_jobs': job_queue.pending_jobs()})

@app.route('/api/job/<string:jid>', methods=['GET', 'DELETE'])
def job_api(jid):
    ''' REST api for getting status, and clearing variables from job '''
    try:
        r = rediscli()
        job_queue = schema.MonacoJobQueue(r)
        if request.method == 'GET':
            job = job_queue.status(jid)
            if job['status'] == 'finished':
                # if the job completed, clear the associated redis structs
                # logs are maintained on the Monaco Nodes, no loss of data
                job_queue.delete_job(jid)
            return jsonify(job)

        if request.method == 'DELETE':
            job_queue.delete_job(jid)
            return 'OK'
    except Exception:
        abort(404)

    abort(400)

@monaco_admin
@app.route('/api/master/migrate', methods=['POST'])
def migrate_app():
    ''' api endpoint to migrate an app '''
    r = rediscli()
    job_queue = schema.MonacoJobQueue(r)
    job = {
        'command': 'migrate',
        'app_id': request.form['app_id'],
        'node_id_from': request.form['node_id_from'],
        'node_id_to': request.form['node_id_to'],
    }
    jid = job_queue.pushback_job(job)
    return jsonify(jid=jid)

@monaco_admin
@app.route('/api/system_health', methods=['GET'])
def system_health():
    r = rediscli()
    res, out = monacotests.run_system_checks(r.connection_pool.connection_kwargs['host'])
    return jsonify({'result': res, 'output': out})
