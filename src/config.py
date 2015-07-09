#!/usr/bin/python
# coding=utf-8
'''
Defines config.config -- a dict with all config values
Defines methods to load/reload the config, set up loggers
(should only be called from daemon.py)
'''
from __future__ import print_function, division

import logging
import logging.handlers
import yaml
import socket
import fcntl
import struct
import sys

# For default exposure
import expose
# For default statpublish
import statpublish

CONFIG_FILE = '/etc/monaco/monaco.conf'

loglevels = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warn': logging.WARN,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
}

# Default values are primarily for documenting config options
# PROVIDE A CONF FILE!!!
config = {
    'env': 'test',
    'location': 'els',
    'loglevel': 'debug',                                    # 'debug', 'info', 'warn', 'error'
    'hostname': 'localhost',
    'IP': '1.2.3.4',                                        # always overwritten
    'enable-failover': True,                                # if False, master daemon won't perform failovers
    'admin_email': 'admin@monaco',
    'smtp-server': 'smtp.yourhost.com',
    'mgmt_port': 6378,                                      # port of the localhost redis-server that holds the MGMT db

    'monaco': {
        'logfile': '/var/log/monaco/monaco.log',
        'monaco-app-confs': '/etc/redis.d',
        'redis-conf': '/etc/redis/redis.conf',
        'memory_utilization': 0.6,
    },

    'network': {
        'device': 'eth0',
    },

    'raft': {
        'cluster': [
            ['127.0.0.1', 5005],
        ],
        'port': 5005,
        'actor': True,
    },

    # This must be loadable module that defines an Expose class
    # import <'expose'>
    # exposure_class = <'expose'>.Expose
    'exposemod': 'expose',
    # this value is auto-filled
    'expose': expose.Expose,

    # This must be a loadable module that defines a Publish class
    # statpublisher = __import__(config['statpublishermod'])
    # publish_class = statpublisher.Publish
    'statpublishermod': 'redis_stats',
    'statpublisher': statpublish.Publish,

    'stats': {
        'interval': 10,
    }
}
logger = logging.getLogger('monaco')

# Methods for top-level daemon thread only
def initLoggers():
    '''
    Initializes the loggers as specified in the config
    '''
    global logger
    formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('monaco')
    logger.setLevel(logging.DEBUG)

    # SMTP logging for production servers
    if config['env'] == 'prod':
        smtphandler = logging.handlers.SMTPHandler(
            config['smtp-server'],
            'monaco@%s.com' % config['hostname'],
            config['admin_email'],
            'ERROR! %s' % config['hostname'],
        )

        smtphandler.setLevel(logging.ERROR)
        smtphandler.setFormatter(formatter)
        logger.addHandler(smtphandler)

    try:
        # set up file logging now that we're config'd
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        filehandler = logging.handlers.WatchedFileHandler(config['monaco']['logfile'])
        filehandler.setLevel(loglevels.get(config['loglevel'], logging.INFO))
        filehandler.setFormatter(formatter)
        logger.addHandler(filehandler)
        # for all the apschedulers out there
        schedlogger = logging.getLogger('apscheduler')
        schedlogger.setLevel(logging.WARNING)
        schedlogger.addHandler(filehandler)
        logger.info('Set up loggers')
        if not config['loglevel'] in loglevels:
            logger.warn('Loglevel %s unknown, using logging.INFO', config['loglevel'])
    except Exception as exc:
        logger.error(exc)
        return False
    return True

# Helpers
def get_ip_address(ifname):
    '''
    Helper method for initCfg
    Gets IP address eth device `ifname` is bound to
    '''
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        sock.fileno(),
        0x8915, # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24])

def confmerge(user, default):
    '''
    Helper method for initCfg
    merges conf dicts, using user of default
    '''
    if isinstance(user, dict) and isinstance(default, dict):
        for k, v in default.iteritems():
            if k not in user:
                user[k] = v
            else:
                user[k] = confmerge(user[k], v)
    return user if not user is None else default

'''
Initializes the config dict from the config file
'''
try:
    with open(CONFIG_FILE) as conf:
        userconf = yaml.load(conf)
        config = confmerge(userconf, config)
except:
    logger.exception('Exception loading user config')

config['hostname'] = socket.gethostname()
try:
    config['IP'] = get_ip_address(config['network']['device'])
except IOError:
    logger.exception('Invalid network device specified: %s', config['network']['device'])
    sys.exit(1)

# set the exposure class
exposemod = __import__(config['exposemod'])
assert issubclass(exposemod.Expose, expose.Expose)
config['expose'] = exposemod.Expose

# set the stat publisher class
statpublishermod = __import__(config['statpublishermod'])
assert issubclass(statpublishermod.Publish, statpublish.Publish)
config['statpublisher'] = statpublishermod.Publish
