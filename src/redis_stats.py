#!/usr/bin/python
'''
Defines a simple stat publisher that uses db1 of the mgmt db to store limited recent data
This provides stats out of the box, but if long term collection is desired, this should be atleast subclassed
To allow for simple UI display, as well as any other collection/aggregation desired
'''
import redis
import time
import logging

import statpublish
from config import config

class Publish(statpublish.Publish):
    '''
    Uploads stats to db=1 of the mgmt db (mgmt uses db=0)
    Intended to only keep a window of recent results (it will actively purge older results)
    '''
    ## zadd 'stat.mgmt.<node_id>.<stat_name>' time.time() <stat_value>
    MONACO_STAT_SCHEME = 'stat.mgmt.%s.%s'
    ## zadd 'stat.app.<app_id>.<stat_name>' time.time() <stat_value>
    APP_STAT_SCHEME = 'stat.app.%s.%s'
    ## zadd 'stat.twem.<twem_id>.<node_id>.<stat_name>' time.time() <stat_value>
    TWEM_STAT_SCHEME = 'stat.twem.%s.%s.%s'

    def __init__(self):
        self.set_rmaster()
        self.window = 60 * 60 * 12 # 12 hours in secs
        self.logger = logging.getLogger('monaco.stats')

    def set_rmaster(self):
        self.rmaster = redis.StrictRedis(port=config['mgmt_port'], db=1)
        info = self.rmaster.info()
        if info['role'] != 'master':
            self.rmaster = redis.StrictRedis(host=info['master_host'], port=config['mgmt_port'], db=1)
        assert self.rmaster.info()['role'] == 'master'

    def _publish_monaco_stat(self, node_id, stat, value):
        ts = int(time.time()) # round to second
        self.rmaster.zadd(Publish.MONACO_STAT_SCHEME % (node_id, stat), ts, '%s:%s' % (value, ts))
        self.rmaster.zremrangebyscore(Publish.MONACO_STAT_SCHEME % (node_id, stat), '-inf', ts - self.window)

    def publish_monaco_stat(self, node_id, stat, value):
        try:
            self._publish_monaco_stat(node_id, stat, value)
        except redis.RedisError:
            self.logger.exception('Exception publishing monaco stat')
            self.set_rmaster()

    def _publish_app_stat(self, app_id, stat, value):
        ts = int(time.time()) # round to second
        self.rmaster.zadd(Publish.APP_STAT_SCHEME % (app_id, stat), ts, '%s:%s' % (value, ts))
        self.rmaster.zremrangebyscore(Publish.APP_STAT_SCHEME % (app_id, stat), '-inf', ts - self.window)

    def publish_app_stat(self, app_id, stat, value):
        try:
            self._publish_app_stat(app_id, stat, value)
        except redis.RedisError:
            self.logger.exception('Exception publishing app stat')
            self.set_rmaster()

    def _publish_twem_stat(self, twem_id, node_id, stat, value):
        ts = int(time.time()) # round to second
        self.rmaster.zadd(Publish.TWEM_STAT_SCHEME % (twem_id, node_id, stat), ts, '%s:%s' % (value,ts))
        self.rmaster.zremrangebyscore(Publish.TWEM_STAT_SCHEME % (twem_id, node_id, stat), '-inf', ts - self.window)

    def publish_twem_stat(self, twem_id, node_id, stat, value):
        try:
            self._publish_twem_stat(twem_id, node_id, stat, value)
        except redis.RedisError:
            self.logger.exception('Exception publishing twem stat')
            self.set_rmaster()
