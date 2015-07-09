#!/usr/bin/python
# coding=utf-8
'''
Monaco - Redis as a Service
'''
from __future__ import print_function, division

from apscheduler.scheduler import Scheduler
import atexit
import logging
import redis
import time
import signal
import os
import sys

import config
from slave import Slave
from master import Master
from lifeRaft import LifeRaft


def monaco():
    '''
    The management daemon for Monaco
    '''
    # Set up top-level console logger
    consolehandler = logging.StreamHandler()
    consolehandler.setLevel(logging.ERROR)
    consolehandler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
    logger = logging.getLogger('monaco')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(consolehandler)

    # Load config
    config.initLoggers()

    # Install reload handler
    def USR1_handler(signum, frame):
        ''' SIGUSR1 handler '''
        logger.info('Received SIGUSR1, reloading Monaco...')
        # this kills the current process tree and re-executes the
        # original call arguments in the same PID (good for upstart)
        os.execl(sys.executable, *([sys.executable]+sys.argv))
    signal.signal(signal.SIGUSR1, USR1_handler)

    # Setup scheduling
    sched = Scheduler(daemon=True)
    atexit.register(lambda: sched.shutdown(wait=False))
    sched.start()

    # Config and start slave thread to maintain app state
    slave = Slave()
    slave.update_subs()

    #slave.start() starting in apsched inherits daemon=True
    @sched.interval_schedule(seconds=1)
    def maintain_slave():
        '''
        This periodically ensures that the slave is subscribed to all the
        channels it should be. Again, this should be redundant
        '''
        if not hasattr(maintain_slave, 'started'):
            maintain_slave.started = True
            slave.start()
            logger.info('Slave thread started')
        if not slave.is_alive():
            logger.info('Slave thread died')
            slave.start()

    # lifeRaft - Use Raft algorithm to maintain management leader
    liferaft = LifeRaft()

    # Start master/monitor thread
    master = Master(liferaft)
    master.start()

    # maintain mgmt redis role from LifeRaft
    r = redis.StrictRedis(port=config.config['mgmt_port'])
    mastercli = None
    while True:
        time.sleep(1)
        try:
            master_tup = liferaft.value
            if not master_tup:
                logger.warn("Couldn't learn master value!")
            else:
                logger.debug('MGMT DB maintaining: %s', repr(master_tup))
                if master_tup[0] == config.config['IP']:
                    if r.info()['role'] == 'master':
                        # We're the master, and properly configured
                        continue
                    # Else set to master, rewrite config to persist
                    logger.debug('Promoting self to master')
                    r.slaveof()
                    r.config_rewrite()
                else:
                    if r.info()['role'] == 'slave' and r.info()['master_host'] == master_tup[0]:
                        # We're a slave and properly configured to the master
                        continue
                    for idx in xrange(3):
                        if idx != 0:
                            # Decongest on retry
                            time.sleep(0.5)
                        try:
                            # Try to get a connection to the mgmt master and verify it thinks it's master
                            if not mastercli:
                                mastercli = redis.StrictRedis(host=master_tup[0], port=config.config['mgmt_port'])
                            if mastercli.info()['role'] != 'master':
                                # If it doesn't think it's master, delete client and continue to retry
                                del mastercli
                                continue
                            break
                        except Exception:
                            continue
                    else:
                        # We didn't break
                        logger.debug('Assigned master (%s) has not yet assumed role', master_tup[0])
                        continue
                    # Set slave to proper master
                    logger.debug('Assigning self as slaveof %s:6379', master_tup[0])
                    r.slaveof(host=master_tup[0], port=config.config['mgmt_port'])
        except Exception:
            r = redis.StrictRedis(port=config.config['mgmt_port'])

if __name__ == '__main__':
    monaco()
