#!/usr/bin/python
# coding=utf-8
'''
Provides Monaco's interface
'''
from __future__ import absolute_import, print_function, division

from flask import render_template, abort

import schema
from web import app, rediscli, monaco_admin, list_apps


@app.route('/health_check')
def health_check():
    '''health check'''
    return 'OK'

@app.route('/')
def index():
    ''' index '''
    return dbs()

@app.route('/dbs')
def dbs():
    ''' index '''
    return render_template(
        'db_index.html',
    )

@app.route('/proxies')
def proxies():
    ''' index '''
    return render_template(
        'proxy_index.html',
        dbs=list_apps(),
    )

# DB View
@app.route('/app/<int:app_id>', methods=['get'])
def app_view(app_id):
    ''' Web UI for an App '''
    r = rediscli()
    monaco = schema.Monaco()
    monaco.refresh(r)
    if not str(app_id) in monaco.app_ids:
        abort(404)
    dbapp = schema.App(app_id=app_id)
    dbapp.refresh(r)
    data = {}
    for node, role in dbapp.nodes.iteritems():
        node = schema.MonacoNode(node_id=node)
        node.refresh(r)
        if role == 'master':
            data[role] = {'host': node.hostname, 'port': dbapp.port}
        elif role in data:
            data[role].append({'host': node.hostname, 'port': dbapp.port})
        else:
            data[role] = [{'host': node.hostname, 'port': dbapp.port}]
    data['app_id'] = app_id
    data['name'] = dbapp.name
    # scale bytes to human readable mb/gb
    data['maxmemory'] = dbapp.maxmemory
    data['maxmemory_policy'] = dbapp.maxmemory_policy
    data['persist'] = dbapp.persist == 'True'
    data['replicas'] = dbapp.replicas
    data['slavelb'] = dbapp.slavelb == 'True'
    data['owner'] = dbapp.owner
    data['operator'] = dbapp.operator

    data['memory_target'] = '&target=monaco.%s.%s.%s.used_memory' % (
        app.config['ENV'],
        app.config['LOCATION'],
        app_id,
    )
    data['rps_target'] = '&target=monaco.%s.%s.%s.instantaneous_ops_per_sec' % (
        app.config['ENV'],
        app.config['LOCATION'],
        app_id,
    )
    data['conn_target'] = '&target=monaco.%s.%s.%s.connected_clients' % (
        app.config['ENV'],
        app.config['LOCATION'],
        app_id,
    )
    data['cpu_target'] = '&target=monaco.%s.%s.%s.cpu_percent' % (
        app.config['ENV'],
        app.config['LOCATION'],
        app_id,
    )
    return render_template('db.html', **data)

# Proxy View
@app.route('/proxy/<int:twem_id>', methods=['get'])
def proxy_view(twem_id):
    ''' Templates the proxy view '''
    r = rediscli()
    monaco = schema.Monaco()
    monaco.refresh(r)
    if not str(twem_id) in monaco.twem_ids:
        abort(404)
    twem = schema.MonacoTwem(twem_id=twem_id)
    twem.refresh(r)
    data = {}
    data['twem_id'] = twem_id
    data['name'] = twem.name
    data['servers'] = twem.servers
    data['extservers'] = twem.extservers
    data['dbinfo'] = {}
    for app_id in twem.servers:
        # Get usage info on all backend DBs
        dbapp = schema.App(app_id=app_id)
        dbapp.refresh(r)
        mastercli = dbapp.get_master_connection(r)
        info = mastercli.info()
        used = float(info['used_memory']) / (1024 * 1024)
        total = int(dbapp.maxmemory) // (1024 * 1024)
        percent = round((100 * used) / total, 2)
        used = round(used, 2)

        data['dbinfo'][app_id] = {}
        data['dbinfo'][app_id]['total'] = total
        data['dbinfo'][app_id]['used'] = used
        data['dbinfo'][app_id]['percent'] = percent

    data['distribution'] = twem.distribution
    data['owner'] = twem.owner
    data['operator'] = twem.operator
    # choices for servers
    data['all_servers'] = [app_id for app_id in list_apps()]

    return render_template('proxy.html', **data)

# Admin Views
@monaco_admin
@app.route('/admin')
def admin_view():
    ''' Web view for Admin page '''
    return render_template('admin.html')

@monaco_admin
@app.route('/node/<int:node_id>', methods=['GET'])
def node_view(node_id):
    ''' Web view for MonacoNode '''
    r = rediscli()
    monaco = schema.Monaco()
    monaco.refresh(r)
    node_id = str(node_id)
    if not node_id in monaco.node_ids:
        abort(404)
    node = schema.MonacoNode(node_id=node_id)
    node.refresh(r)
    appinfo = node.app_info(r)
    data = {
        'node_id': node_id,
        'hostname': node.hostname,
        'FQDN': node.FQDN,
        'total_memory': node.total_memory,
        'rack': node.rack,
        'status': node.status,
        'used_memory': appinfo['memory'],
        'memory_percent': round(100.0 * appinfo['memory'] / (int(node.total_memory) / 2.0), 2),
        'master_servers': len(appinfo['masters']),
        'masters': map(str,sorted(map(int,appinfo['masters']))),
        'slave_servers': len(appinfo['slaves']),
        'slaves': map(str,sorted(map(int,appinfo['slaves']))),
        'twemproxy_servers': len(node.twems),
        # General info
        'nodes': map(str,sorted(map(int,monaco.node_ids))),
    }
 
    return render_template("node.html", **data)

if __name__ == '__main__':
    app.run('0.0.0.0', debug=True)
