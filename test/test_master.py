#!/usr/bin/python
# coding=utf-8
'''
Tests schema.py
'''
from __future__ import print_function, division
#import from parent dirs
import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sourcedir = os.path.join(parentdir, 'src')
sys.path.insert(0, sourcedir)

from master import Master

from mock import patch, Mock
import unittest

CONFIG={
    'env': 'test',
    'location': 'els',
    'loglevel': 'debug',                                    # 'debug', 'info', 'warn', 'error'
    'hostname': 'localhost',
    'IP': '1.2.3.4',                                        # always overwritten
    'enable-failover': True,                                # if False, master daemon won't perform failovers
    'mgmt_port': 6379,                                      # port of the localhost redis-server that holds the MGMT db

    'monaco': {
        'logfile': '/var/log/monaco/monaco.log',
        'monaco-app-confs': '/etc/redis.d',
        'redis-conf': '/etc/redis/redis.conf',
        'memory_utilization': 0.6,
    },
    'expose': Mock(),
}

class MasterBaseTest(unittest.TestCase):
    '''
    Defines setUp/tearDown
    '''
    def setUp(self):
        '''
        Mock redis constructors, AsyncPubSub constructor, schema methods, and config file
        '''
        self.redis_cli_mock = Mock()
        self.redis_mock = Mock(return_value=self.redis_cli_mock)
        self.pubsub_cli_mock = Mock()
        self.pubsub_mock = Mock(return_value=self.pubsub_cli_mock)
        with patch('master.config', CONFIG, create=True):
            with patch('redis.StrictRedis', self.redis_mock, create=True):
                with patch('master.AsyncPubSub', self.pubsub_mock, create=True):
                    with patch('schema.Monaco', create=True):
                        with patch('schema.MonacoJobQueue', create=True):
                            self.lp_mock = Mock()
                            self.master = Master(self.lp_mock)

    def tearDown(self):
        pass

    def test_init(self):
        self.assertEquals(self.master.r, self.redis_cli_mock)
        self.assertEquals(self.master.rps, self.redis_cli_mock)
        self.assertEquals(self.master.pubsub, self.pubsub_cli_mock)

class MasterAPITests(MasterBaseTest):
    '''
    Tests expected functionality of API Methods
    '''
    def test_api_methods(self):
        self.assertEquals(len(self.master.api_handlers), 9)

    def test_ping(self):
        self.assertTrue(self.master.api_ping(None, Mock()))

    def test_new_essential_field_missing(self):
        job = {
            'name': 'newname',
        }
        self.assertFalse(self.master.api_new(job, Mock()))

    def test_new_app_exists(self):
        job = {
            'name': 'newname',
            'maxmemory': '1024', 
            'maxmemory_policy': 'noevict',
            'persist': False,
            'replicas': 1,
            'owner': 'Keith Ainsworth',
            'operator': 'Hulu',
        }
        self.master.monaco.app_ids_by_service = {'newname': 6380}
        self.assertFalse(self.master.api_new(job, Mock()))

    def test_new_no_cluster(self):
        job = {
            'name': 'newname',
            'maxmemory': '1024', 
            'maxmemory_policy': 'noevict',
            'persist': False,
            'replicas': 1,
            'owner': 'Keith Ainsworth',
            'operator': 'Hulu',
        }
        self.master.monaco.app_ids_by_service = {}
        self.master.monaco.next_app_id = Mock(return_value = 6380)
        with patch.object(self.master, 'select_cluster', Mock(return_value=None), create=True):
            self.assertFalse(self.master.api_new(job, Mock()))

    def test_update_nonexistent(self):
        ''' This raises an assertion '''
        job = {'app_id': 6380}
        self.master.apphandlerlocks = []
        exception = False
        try:
            self.master.api_update(job, Mock())
        except:
            exception = True
        self.assertTrue(exception)

    def test_update_unlocked(self):
        ''' til; How to mock a context generator '''
        job = {'app_id': 6380}
        self.master.apphandlerlocks = {6380: None}
        nblc = Mock()
        nblc.__enter__ = Mock(return_value=False)
        nblc.__exit__ = Mock()
        nblc_mock = Mock(return_value=nblc)
        with patch('master.NBLockContext', nblc_mock, create=True):
            self.assertFalse(self.master.api_update(job, Mock()))

    def test_migrate_nonorigin(self):
        ''' migrate from node not in cluster '''
        job = {
            'app_id': 6380,
            'node_id_from': 'elephant',
        }
        app = Mock()
        app.nodes = ['tiger', 'moth']
        app.refresh = Mock()
        app_mock = Mock(return_value=app)
        with patch('schema.App', app_mock, create=True):
            self.assertFalse(self.master.api_migrate(job, Mock()))
        app.refresh.assert_called_once_with(self.master.r)

    def test_migrate_nochange(self):
        ''' migrate from node not in cluster '''
        job = {
            'app_id': 6380,
            'node_id_from': 'elephant',
            'node_id_to': 'elephant',
        }
        app = Mock()
        app.nodes = ['tiger', 'moth']
        app.refresh = Mock()
        app_mock = Mock(return_value=app)
        with patch('schema.App', app_mock, create=True):
            self.assertFalse(self.master.api_migrate(job, Mock()))
        app.refresh.assert_called_once_with(self.master.r)

    def test_migrate_slaveswap(self):
        ''' migrate from node not in cluster '''
        job = {
            'app_id': 6380,
            'node_id_from': 'elephant',
            'node_id_to': 'tiger',
        }
        app = Mock()
        app.nodes = {
            'elephant': 'slave',
            'tiger': 'slave',
            'moth': 'master',
        }
        app.refresh = Mock()
        app_mock = Mock(return_value=app)
        with patch('schema.App', app_mock, create=True):
            self.assertFalse(self.master.api_migrate(job, Mock()))
        app.refresh.assert_called_once_with(self.master.r)

    def test_migrate_slave2master(self):
        ''' migrate from node not in cluster '''
        job = {
            'app_id': 6380,
            'node_id_from': 'elephant',
            'node_id_to': 'moth',
        }
        app = Mock()
        app.nodes = {
            'elephant': 'slave',
            'tiger': 'slave',
            'moth': 'master',
        }
        app.refresh = Mock()
        app_mock = Mock(return_value=app)
        with patch('schema.App', app_mock, create=True):
            self.assertFalse(self.master.api_migrate(job, Mock()))
        app.refresh.assert_called_once_with(self.master.r)

    


def suite():
    suite = unittest.TestSuite()

    # Verify Mock'd master is created properly
    suite.addTest(MasterBaseTest("test_init"))

    # Verify API functionality of master
    # (primarily invarience/edge case testing for regression avoidance)
    suite.addTest(MasterAPITests("test_api_methods"))
    suite.addTest(MasterAPITests("test_ping"))
    suite.addTest(MasterAPITests("test_new_essential_field_missing"))
    suite.addTest(MasterAPITests("test_new_app_exists"))
    suite.addTest(MasterAPITests("test_new_no_cluster"))
    suite.addTest(MasterAPITests("test_update_nonexistent"))
    suite.addTest(MasterAPITests("test_update_unlocked"))
    suite.addTest(MasterAPITests("test_migrate_nonorigin"))
    suite.addTest(MasterAPITests("test_migrate_nochange"))
    suite.addTest(MasterAPITests("test_migrate_slaveswap"))
    suite.addTest(MasterAPITests("test_migrate_slave2master"))

    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    test_suite = suite()
    runner.run(test_suite)
