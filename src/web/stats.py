#!/usr/bin/python
# coding=utf-8
'''
Provides Monaco's internal stats exposure
'''
from __future__ import absolute_import, print_function, division

from redis import StrictRedis, TimeoutError
from flask import request, jsonify

from web import app

# Stats are stored in sorted sets: zscan for --> (weight: time, val: stat_value)
MONACO_STAT_FMT = 'stat.mgmt.%s.%s'     # % (node_id, stat_name)
APP_STAT_FMT = 'stat.app.%s.%s'         # % (app_id, stat_name)
TWEM_STAT_FMT = 'stat.twem.%s.%s.%s'    # % (node_id, twem_id, stat_name)


def statscli(master=True):
    '''
    Like __init__.py:rediscli() except for db=1
    '''
    for hostname in app.config.get('MONACO_HOSTS', ['localhost']):
        try:
            r = StrictRedis(host=hostname, port=app.config['MGMT_PORT'], db=1, socket_connect_timeout=1)
            info = r.info()
            if info['role'] == 'master' or not master:
                return r
            else:
                return StrictRedis(host=info['master_host'], port=app.config['MGMT_PORT'], db=1, socket_connect_timeout=1)
        except TimeoutError:
            continue
    raise TimeoutError('Timeout connection to monaco stats db')


@app.route('/stats/mgmt/<int:node_id>/<stat_name>')
def get_mgmt_stat(node_id, stat_name):
    ''' Gets mgmt db stats for a specific node '''
    import time
    r = statscli(master=False)
    stat_key = MONACO_STAT_FMT % (node_id, stat_name)
    end_time = time.time()
    start_time = end_time - (60 * 60)
    if 'start_time' in request.args:
        start_time = request.args['start_time']
    if 'end_time' in request.args:
        end_time = request.args['end_time']
    data = r.zrangebyscore(stat_key, start_time, end_time, withscores=True)
    data = [{'x': time, 'y': float(val.split(':')[0])} for (val, time) in data]
    return jsonify({'service': 'monaco', 'node_id': node_id, 'stat_name': stat_name, 'from': start_time, 'to': end_time, 'data': data})

@app.route('/stats/app/<int:app_id>/<stat_name>')
def get_app_stat(app_id, stat_name):
    ''' Gets an app stat '''
    r = statscli(master=False)
    stat_key = APP_STAT_FMT % (app_id, stat_name)
    import time
    end_time = time.time()
    start_time = end_time - (60 * 60)
    if 'start_time' in request.args:
        start_time = request.args['start_time']
    if 'end_time' in request.args:
        end_time = request.args['end_time']
    data = r.zrangebyscore(stat_key, start_time, end_time, withscores=True)
    data = [{'x': time, 'y': float(val.split(':')[0])} for (val, time) in data]
    return jsonify({'app_id': app_id, 'stat_name': stat_name, 'from': start_time, 'to': end_time, 'data': data})

@app.route('/stats/twem/<int:node_id>/<int:twem_id>/<stat_name>')
def get_twem_stat(node_id, twem_id, stat_name):
    ''' Gets a twem stat for a specific node '''
    r = statscli(master=False)
    stat_key = TWEM_STAT_FMT % (node_id, twem_id, stat_name)
    import time
    end_time = time.time()
    start_time = end_time - (60 * 60)
    if 'start_time' in request.args:
        start_time = request.args['start_time']
    if 'end_time' in request.args:
        end_time = request.args['end_time']
    data = r.zrangebyscore(stat_key, start_time, end_time, withscores=True)
    data = [{'x': time, 'y': float(val.split(':')[0])} for (val, time) in data]
    return jsonify({'twem_id': twem_id, 'node_id': node_id, 'stat_name': stat_name, 'from': start_time, 'to': end_time, 'data': data})

@app.route('/stats/proxy/<int:twem_id>/<stat_name>')
def get_all_twem_stat(twem_id, stat_name):
    ''' Receives a twem stat for all nodes matching this proxy '''
    r = statscli(master=False)
    stat_keys = r.keys(TWEM_STAT_FMT % ('*', twem_id, stat_name))
    import time
    end_time = time.time()
    start_time = end_time - (60 * 60)
    if 'start_time' in request.args:
        start_time = request.args['start_time']
    if 'end_time' in request.args:
        end_time = request.args['end_time']
    data = {
        'twem_id': twem_id,
        'stat_keys': stat_keys,
        'stat_name': stat_name,
        'from': start_time,
        'to': end_time,
    }
    for key in stat_keys:
        data[key] = r.zrangebyscore(key, start_time, end_time, withscores=True)
        data[key] = [{'x': time, 'y': float(val.split(':')[0])} for (val, time) in data[key]]
    return jsonify(data)
