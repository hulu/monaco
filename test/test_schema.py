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

import schema

from mock import patch
import unittest

class SchemaMonacoNodeTests(unittest.TestCase):
    '''
    MonacoNode status methods take a redis client as input
    We mock a redis client and use that instead
    '''
    def setUp(self):
        self.monaconode = schema.MonacoNode(node_id=1)

    def test_status_down(self):
        self.monaconode.status = 'UP'
        self.monaconode.hb_count = 0
        with patch.object(self.monaconode, 'refresh'):
            with patch.object(self.monaconode, 'write_hash') as wh_mock:
                self.assertFalse(self.monaconode.status_down(None))
        wh_mock.assert_called_once_with(None)
        self.assertEquals(self.monaconode.hb_count, 1)

    def test_status_down_full(self):
        self.monaconode.status = 'UP'
        self.monaconode.hb_count = 0
        with patch.object(self.monaconode, 'refresh'):
            with patch.object(self.monaconode, 'write_hash'):
                self.assertFalse(self.monaconode.status_down(None))
                self.assertEquals(self.monaconode.hb_count, 1)
                self.assertFalse(self.monaconode.status_down(None))
                self.assertEquals(self.monaconode.hb_count, 2)
                self.assertEquals(self.monaconode.status, 'UP')
                self.assertTrue(self.monaconode.status_down(None))
                self.assertEquals(self.monaconode.hb_count, 3)
                self.assertEquals(self.monaconode.status, 'DOWN')

    def test_status_up_reset_count(self):
        self.monaconode.status = 'UP'
        self.monaconode.hb_count = 0
        with patch.object(self.monaconode, 'refresh'):
            with patch.object(self.monaconode, 'write_hash'):
                self.assertFalse(self.monaconode.status_down(None))
                self.assertEquals(self.monaconode.hb_count, 1)
                self.assertFalse(self.monaconode.status_down(None))
                self.assertEquals(self.monaconode.hb_count, 2)
                self.assertTrue(self.monaconode.status_up(None))
                self.assertEquals(self.monaconode.hb_count, 0)
                self.assertEquals(self.monaconode.status, 'UP')

    def test_status_up_reset_status(self):
        self.monaconode.status = 'DOWN'
        self.monaconode.hb_count = 3
        with patch.object(self.monaconode, 'refresh'):
            with patch.object(self.monaconode, 'write_hash'):
                self.assertFalse(self.monaconode.status_down(None))
                self.assertEquals(self.monaconode.hb_count, 4)
                self.assertTrue(self.monaconode.status_up(None))
                self.assertEquals(self.monaconode.hb_count, 0)
                self.assertEquals(self.monaconode.status, 'UP')

    def test_status_maint_blocks_down(self):
        self.monaconode.status = 'UP'
        self.monaconode.hb_count = 2
        with patch.object(self.monaconode, 'refresh'):
            with patch.object(self.monaconode, 'write_hash'):
                self.assertTrue(self.monaconode.status_maintenance(None))
                self.assertEquals(self.monaconode.hb_count, 0)
                self.assertEquals(self.monaconode.status, 'MAINTENANCE')
                self.assertFalse(self.monaconode.status_down(None))
                self.assertEquals(self.monaconode.hb_count, 0)
                self.assertEquals(self.monaconode.status, 'MAINTENANCE')

    def test_status_maint_blocks_up(self):
        self.monaconode.status = 'DOWN'
        self.monaconode.hb_count = 3
        with patch.object(self.monaconode, 'refresh'):
            with patch.object(self.monaconode, 'write_hash'):
                self.assertTrue(self.monaconode.status_maintenance(None))
                self.assertEquals(self.monaconode.hb_count, 0)
                self.assertEquals(self.monaconode.status, 'MAINTENANCE')
                self.assertFalse(self.monaconode.status_up(None))
                self.assertEquals(self.monaconode.hb_count, 0)
                self.assertEquals(self.monaconode.status, 'MAINTENANCE')

    def test_force_status_up(self):
        self.monaconode.status = 'MAINTENANCE'
        self.monaconode.hb_count = 0
        with patch.object(self.monaconode, 'refresh'):
            with patch.object(self.monaconode, 'write_hash'):
                self.assertTrue(self.monaconode.status_up(None, force=True))
                self.assertEquals(self.monaconode.hb_count, 0)
                self.assertEquals(self.monaconode.status, 'UP')


def suite():
    suite = unittest.TestSuite()

    # MonacoNode status component tests
    suite.addTest(SchemaMonacoNodeTests("test_status_down"))
    suite.addTest(SchemaMonacoNodeTests("test_status_down_full"))
    suite.addTest(SchemaMonacoNodeTests("test_status_up_reset_count"))
    suite.addTest(SchemaMonacoNodeTests("test_status_up_reset_status"))
    suite.addTest(SchemaMonacoNodeTests("test_status_maint_blocks_down"))
    suite.addTest(SchemaMonacoNodeTests("test_status_maint_blocks_up"))
    suite.addTest(SchemaMonacoNodeTests("test_force_status_up"))



    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    test_suite = suite()
    runner.run(test_suite)
