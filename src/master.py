#!/usr/bin/python
# coding=utf-8
'''
Provides the master role of monaco.
This uses a redis queue to accept commands from UI/anywhere else they'd come
to then enact those changes in the mgmt db, propagating instances in the cluster
Also, this is responsible for maintaining system health, and performing app failovers
'''
from __future__ import print_function, division

import redis
import time
import threading
import atexit
import logging
import smtplib
import re
from apscheduler.scheduler import Scheduler
from email.mime.text import MIMEText

from config import config
import schema
from async_pubsub import AsyncPubSub
from nblockcontext import NBLockContext

def sendmail(toaddr, fromaddr, subj, msg, server):
    ''' Sends an email message via SMTP server
        To: <toaddr>,
        From: <fromaddr>,
        Subject: <subj>,
        <msg>
    '''
    if type(toaddr) != list:
        toaddr = [toaddr]
    msg = MIMEText(msg)
    msg['To'] = ', '.join(toaddr)
    msg['From'] = fromaddr
    msg['Subject'] = subj

    smtpserv = smtplib.SMTP(server)
    smtpserv.sendmail(fromaddr, toaddr, msg.as_string())
    smtpserv.quit()

class Master(threading.Thread):
    '''
    A master thread, which can be elevated to acting master, or slave
    '''
    API_FUNCTIONS = [
        'new',
        'update',
        'delete',
        'migrate',
        'maintenance',
        'new_proxy',
        'update_proxy',
        'delete_proxy',
        'ping',
    ]

    def __init__(self, liferaft):
        super(Master, self).__init__()
        self.daemon = True

        self.role = 'slave'
        self.logger = logging.getLogger('monaco.master')
        self.node_ip = config['IP']
        self.liferaft = liferaft
        self.hostname = config['hostname']
        self.memory_utilization = config['monaco']['memory_utilization']
        self.expose = config['expose']()

        self.monaco = schema.Monaco()
        self.r = redis.StrictRedis(port=config['mgmt_port'])
        self.job_queue = schema.MonacoJobQueue(self.r)
        self.sched = Scheduler(daemon=True)
        self.sched.start()
        atexit.register(lambda: self.sched.shutdown(wait=False))

        # Set up listener thread
        self.rps = redis.StrictRedis(port=config['mgmt_port'])
        self.pubsub = AsyncPubSub(self.rps.connection_pool, ignore_subscribe_messages=True)
        #self.pubsub = self.rps.pubsub(ignore_subscribe_messages=True)
        self._subscriptions = {}
        # so API functions can "pause" failover at the app level when modifying apps
        self.apphandlerlocks = {} # app_id -> threading.Lock()

        # Set up API handlers
        self.api_handlers = {}
        for func in self.API_FUNCTIONS:
            if not hasattr(self, 'api_' + func):
                self.logger.error('Unhandled API function %s (no class method api_%s)', func, func)
            else:
                self.api_handlers[func] = getattr(self, 'api_' + func)
        # API DB access lock
        self.db_lock = threading.Lock()

    def update_subs(self):
        '''
        Subscribes the master to the status object of all apps
        '''
        try:
            channellist = [schema.keysub(schema.MONACO_JOB_QUEUE)]
            for app_id in self.monaco.app_ids:
                if app_id not in self.apphandlerlocks:
                    self._add_subscription(schema.keysub(schema.APP_NODESTATUS_TMPL % app_id), self.app_status_handler_factory(app_id))
                channellist.append(schema.keysub(schema.APP_NODESTATUS_TMPL % app_id))
            self._add_subscription(schema.keysub(schema.MONACO_JOB_QUEUE), self.web_api_handler_factory())
            map(self._remove_subscription, [chan for chan in self._subscriptions.keys() if not chan in channellist])
        except redis.RedisError, err:
            self.logger.exception(err)
            self.r = redis.StrictRedis(port=config['mgmt_port'])

    def _add_subscription(self, channel, handler):
        '''
        Adds a handler to a channel on the pubsub listener
        '''
        if channel in self._subscriptions:
            if self._subscriptions[channel] != handler:
                self.pubsub.subscribe(**{channel: handler})
                if not channel in self.pubsub.channels:
                    self.logger.error('Failed to subscribe to %s', channel)
                    raise RuntimeError('Failed to subscribe to %s' % channel)
                self._subscriptions[channel] = handler
        else:
            self.pubsub.subscribe(**{channel: handler})
            if not channel in self.pubsub.channels:
                self.logger.error('Failed to subscribe to %s', channel)
                raise RuntimeError('Failed to subscribe to %s' % channel)
            self._subscriptions[channel] = handler

    def _remove_subscription(self, channel):
        '''
        Removes a handler for a channel on the pubsub listener
        '''
        if channel in self._subscriptions:
            self.pubsub.unsubscribe(channel)
        del self._subscriptions[channel]
        try:
            # This grabs the app_id from the channel subscription
            match = re.match(schema.MONACO_APP_STATUS % '([0-9]+)', channel)
            del self.apphandlerlocks[match.groups(0)]
        except Exception, exc:
            self.logger.info('Failed to remove apphandlerlock for channel %s, (%s)', channel, repr(exc))

    def app_status_handler_factory(self, app_id):
        '''
        Makes a handler for app status change events
        This must first check that anything needs to be done, then perform necessary failover steps
        '''
        # Sanitize to str so the key used for lookup is uniform
        app_id = str(app_id)
        thisapp = schema.App(app_id=app_id)
        handlerlock = threading.Lock()
        # there should never be 2 handlers for the same app
        assert app_id not in self.apphandlerlocks
        self.apphandlerlocks[app_id] = handlerlock

        def app_status_handler(msg):
            '''
            This checks the app status, then orchestrates any changes necessary
            '''
            try: # catch all exceptions
                self.logger.debug('app_status_handler received %s', msg)
                if msg['data'] != 'hset':
                    # don't care about hdel. hset changes status
                    self.logger.debug('Non hset event (%s)', msg['data'])
                    return
                if self.role != 'master':
                    # old subscription? new master?
                    self.logger.warn("I'm no longer the master!")
                    return

                # no NBLockContext here, because failover has to happen
                with handlerlock:
                    # one handler at a time per app, refresh after lock ensures not "double-fixing"
                    thisapp.refresh(self.r)
                    status = thisapp.get_status(self.r)
                    best_slave, best_offset, promote = None, -1, False
                    move = []
                    for node_id, role in thisapp.nodes.iteritems():
                        if not node_id in status['nodes']:
                            if role == 'master':
                                promote = True
                            else:
                                move.append(node_id)
                        elif status[node_id]['state'] != 'run':
                            if role == 'master':
                                promote = True
                            else:
                                move.append(node_id)
                        elif status[node_id]['offset'] > best_offset:
                            best_slave = node_id
                            best_offset = status[node_id]['offset']
                    if (not promote) and len(move) == 0:
                        self.logger.debug('App %s OK', thisapp.app_id)
                        return
                    self.logger.debug('Status: %s\nMaster OK:%s, Best Slave:%s', status, not promote, best_slave)
                    # info collected, now act
                    if not config['enable-failover']:
                        self.logger.critical('FAILOVER(s) IGNORED! App (%s), Master Failure (%s)', thisapp.app_id, promote)
                        return
                    if promote:
                        if len(thisapp.nodes) == 1:
                            # If this is a single master app select a new master node
                            best_slave = self.select_master_node(int(thisapp.maxmemory) // (1024 * 1024))
                        if not best_slave:
                            raise RuntimeError('Failed to select a best slave')
                        self.logger.debug('Detected %s should be promoted to master of %s', best_slave, thisapp.app_id)
                        move.append(thisapp.master)
                        # promote the best_slave to master
                        job = {
                            'app_id': thisapp.app_id,
                            'node_id_to': best_slave,
                            'node_id_from': thisapp.master,
                        }
                        with self.db_lock:
                            if not self.api_migrate(job, self.logger):
                                raise RuntimeError('Failed to fix broken master')
                            else:
                                toaddrs = [
                                    thisapp.operator,
                                    config['admin_email'],
                                ]
                                sendmail(toaddrs,
                                         'monaco@monaco.monaco',
                                         'NOTICE: Unscheduled failover of DB Master (%s)' % thisapp.app_id,
                                         '''
There was an issue detected with the redis-server your DB master shard was allocated to (%s). As a result, Monaco is promoting the most up-to-date slave replica (%s). On completion of the transition, the loadbalanced VIP will be updated to reflect the new location of your master. Your client code may throw exceptions during the transition period but if you have properly implemented reconnect logic, the problem should resolve itself.
The app in reference: %s
Thanks for your understanding''' % (thisapp.master, best_slave, thisapp.app_id),
                                         config['smtp-server']
                                )
                                self.logger.info('Promoted app %s slave %s to master (formerly %s)', job['app_id'], job['node_id_to'], job['node_id_from'])
                    for node_id in move:
                        # select new node as slave
                        size = int(thisapp.maxmemory) // (1024 * 1024) # size in MB
                        newnode_id = self.select_node(size, thisapp.nodes.keys())
                        if not newnode_id:
                            self.logger.error('Failed to select a new slave node for app %s', thisapp.app_id)
                            continue
                        job = {
                            'app_id': thisapp.app_id,
                            'node_id_from': node_id,
                            'node_id_to': newnode_id,
                        }
                        with self.db_lock:
                            if not self.api_migrate(job, self.logger):
                                raise RuntimeError('Failed to fix broken slave')
                            else:
                                self.logger.info('Migrated app %s slave on node %s to node %s', job['app_id'], job['node_id_from'], job['node_id_to'])
            except Exception, exc: # Rule #1 of pubsub- never break the subscriber thread via callback exceptions
                self.logger.exception(exc)

        return app_status_handler

    def assume_master(self):
        '''
        When this node becomes a master, this init's the responsibilities
        '''
        self.logger.info('Assumed role of Monaco Master')
        self.sched.add_interval_job(self.cluster_health, seconds=5)

        self.logger.debug('Subscribing to master events')
        self.update_subs()
        if hasattr(self, '_listener_thread'):
            if self._listener_thread.is_alive():
                return
        else:
            self.logger.info('Launching master event listener')
            self._listener_thread = self.pubsub.run_in_thread(sleep_time=0.1)
        self.logger.debug('Listener thread (running:%s): %s', self._listener_thread.is_alive(), self._listener_thread)

    def drop_master(self):
        '''
        Rarely used function, triggered when a network partition is ammended
        and two 'subclusters' might've elected different masters. Fixed quorum
        size (via fixed cluster) will prevent this scenario entirely.
        '''
        self.logger.info('Dropped role of Monaco Master')
        for job in self.sched.get_jobs():
            self.sched.unschedule_job(job)

        if hasattr(self, '_listener_thread'):
            if self._listener_thread.is_alive():
                self._listener_thread.stop()
            delattr(self, '_listener_thread')

    def cluster_health(self):
        '''
        Polls redis for current cluster status.
        If a node has failed repeatedly, the apps on it will be offloaded
        '''
        if self.role != 'master':
            return
        self.monaco.refresh(self.r)
        offload = []
        for node_id in self.monaco.node_ids:
            node = schema.MonacoNode(node_id=node_id)
            node.refresh(self.r)
            if node.hostname == self.hostname:
                continue
            try:
                nodecli = redis.StrictRedis(host=node.hostname, port=config['mgmt_port'], socket_connect_timeout=1, socket_timeout=1)
                nodecli.info()
                self.monaco.node_up(node_id, self.r)
                self.logger.debug('Node %s is healthy', node_id)
            except redis.RedisError, exc:
                self.logger.warn('Node %s missed a healthcheck', node_id)
                if self.monaco.node_down(node_id, self.r):
                    self.logger.error('Node %s has been marked down', node_id)
                    self.logger.exception(exc)
                    offload.append(node_id)

        for node in offload:
            if config['enable-failover']:
                self.logger.warn('Failing over node: %s', repr(node))
                self.offload_node(node)
            else:
                # Get real scary, incase it was accidentally left off
                self.logger.critical('DISMISSING FAILOVER FOR NODE %s! (disabled)', repr(node))

    def web_api_handler_factory(self):
        '''
        Triggered whenever the job queue changes
        '''
        def web_api_handler(msg):
            '''
            On trigger, this pulls from the job queue, and executes 1 job
            '''
            self.logger.debug('web_api_handler received %s', msg)
            if msg['data'] != 'rpush':
                # we only want to react to enqueue events, not dequeue
                self.logger.debug('non rpush event (%s)', msg['data'])
                return
            try:
                if not self.job_queue.empty:
                    jid, job = self.job_queue.get_next_job()
                else:
                    return
            except redis.RedisError:
                self.logger.error('Exception while querying/reading job/monaco struct information via. redis')
                self.r = redis.StrictRedis(port=config['mgmt_port'])
                return
            except Exception, exc:
                self.logger.error('Exception getting next job...')
                self.logger.exception(exc)
                return

            try:
                if not job['command'] in self.api_handlers:
                    self.logger.warn('Received unhandled command %s', job['command'])
                    return
                # create a custom logger
                joblogger = logging.getLogger('monaco.master.%s' % jid)
                joblogger.setLevel(logging.DEBUG) # for verbose user output
                jobloghandler = schema.MonacoJobHandler(jid, self.r)
                jobloghandler.setFormatter(logging.Formatter('%(name)s:%(asctime)s - %(message)s'))
                joblogger.addHandler(jobloghandler)

                with self.db_lock:
                    if self.api_handlers[job['command']](job, joblogger):
                        self.logger.info('Executed job %s', repr(job))
                        self.job_queue.finish_job(jid, True)
                    else:
                        self.logger.info('Failed to execute job %s', repr(job))
                        self.job_queue.finish_job(jid, False)
            except Exception, exc:
                self.logger.error('Exception handling job %s', repr(job))
                self.logger.exception(exc)
                self.job_queue.log_progress(jid, 'Exception handling job %s\n%s' % (jid, repr(exc)))
                self.job_queue.finish_job(jid, False)

        return web_api_handler

    def run(self):
        '''
        Handles all the work.
        There is a very simplistic api handler, for the sake of maintaining
        atomicity of commands
        '''
        if hasattr(self, '_run'):
            return self._run
        self._run = True

        while self._run:
            # Main loop for master. Coordinate with LP and maintain proper role
            time.sleep(1)
            try:
                self.monaco.refresh(self.r)
            except redis.RedisError:
                self.logger.error('Exception refreshing monaco state')
                self.r = redis.StrictRedis(port=config['mgmt_port'])
                continue
            master = self.liferaft.value
            if not master:
                self.logger.warn('Unable to learn master!')
                if self.role == 'master':
                    self.role = 'slave'
                    self.drop_master()
                continue
            elif master[0] == self.node_ip:
                self.logger.debug('Maintaining master role')
                if self.role != 'master':
                    self.role = 'master'
                    self.assume_master()
                self.update_subs()
            else:
                self.logger.debug('Maintaining slave role')
                if self.role != 'slave':
                    self.role = 'slave'
                    self.drop_master()

    def stop(self):
        ''' Passively attempt to kill the master thread '''
        self._run = False


#### API methods ####
    def api_new(self, job, logger):
        '''
        Handles a new app job, where job must have:
        job = {
            'name': str
            **all fields of schema.App.ESSENTIAL_KEYS
        }
        '''
        app_name = job['name']
        logger.debug('Starting: New DB %s', app_name)
        # create the app structure
        args = {}
        for k in schema.App.HASH_KEYS:
            if not k in job and k in schema.App.ESSENTIAL_KEYS:
                logger.error('Expected field not provided (%s)' % k)
                return False
            if not k in job:
                continue # non-essential key omitted.
            args[k] = job[k]
        if app_name in self.monaco.app_ids_by_service:
            logger.warn('App already exists with given name (%s)', app_name)
            return False
        args['app_id'] = self.monaco.next_app_id(self.r)
        args['revision'] = 1
        newapp = schema.App(**args)
        size = int(newapp.maxmemory) // (1024 * 1024) # size in MB
        newapp.nodes = self.select_cluster(size, int(newapp.replicas))
        if not newapp.nodes:
            # couldn't allocate a cluster for this request
            logger.warn('New db %s could not be allocated due to resources', job['name'])
            return False
        newapp.write(self.r)
        # now append the app to the node's app list to trigger creation
        for node_id in newapp.nodes.keys():
            node = schema.MonacoNode(node_id=node_id)
            node.refresh(self.r)
            node.apps.append(newapp.app_id)
            node.write(self.r)
            logger.debug('Propagating app %s to node %s', newapp.app_id, node_id)
        # wait for all nodes to report running revision 1
        logger.debug('Waiting for all nodes to load new DB...')
        try:
            if not newapp.wait_status(self.r, revision=1):
                logger.error('Failed to acheive consistent revision across new cluster. Do something I guess?')
        except Exception, exc:
            logger.exception(exc) # while debugging this
        # configure the exposure method
        logger.debug('Creating exposure')
        if not self.expose.create_app(newapp):
            logger.error('Failed to create exposure')
        # optionally configure the slave loadbalancer endpoint
        if newapp.slavelb == 'True':
            logger.debug('Creating slave exposure')
            if not self.expose.create_app_slave(newapp):
                logger.error('Failed to create slave exposure')
        # now add the app to the top-level monaco struct to indicate creation
        self.monaco.new_app(app_name, newapp.app_id, self.r)
        self.logger.info('New app %s (%s)', job['name'], newapp.app_id)
        logger.debug('Subscribing master to failover events')
        self.update_subs()
        return True

    def api_update(self, job, logger):
        '''
        Handles a config update- essentially same as api_new
        job = {
            'app_id': app_id
            **all fields or partial subset of schema.App.HASH_KEYS
        }
        '''
        # load the app
        app = schema.App(app_id=job['app_id'])
        # can't update an app that doesn't exist yet
        assert app.app_id in self.apphandlerlocks
        # 'pause' app monitoring while updating
        with NBLockContext(self.apphandlerlocks[app.app_id], timeout=5) as acquired:
            if not acquired:
                logger.error('Failed to acquire applock in 5s!\n' \
                             'This could mean there was another api command still running, or a failover')
                return False
            app.refresh(self.r)
            replicas = int(app.replicas)
            logger.debug('Updating dynamic config settings...')
            oldslavelb = app.slavelb == 'True'
            # update options
            for k in app.HASH_KEYS:
                if k in job:
                    setattr(app, k, job[k])
            # Increment the revision number
            app.update_revision()
            # write app to persist changes
            app.write_hash(r=self.r)

            cluster_changed = False
            # update cluster
            if int(job['replicas']) > replicas:
                # add slaves till replicas met
                logger.debug('Adding slave nodes to cluster')
                size = int(app.maxmemory) // (1024 * 1024) # size in MB
                for _ in xrange(int(job['replicas']) - replicas):
                    node_id = self.select_node(size, app.nodes.keys())
                    app.nodes[node_id] = 'slave'
                    app.write_cluster(self.r)
                    node = schema.MonacoNode(node_id=node_id)
                    node.refresh(self.r)
                    node.apps.append(app.app_id)
                    node.write(self.r)
                    logger.debug('Added (%s)', node_id)
                cluster_changed = True
            elif int(job['replicas']) < replicas:
                # remove the app from as many slaves as necessary
                logger.debug('Removing slave nodes from cluster')
                slave_nodes = [k for k, v in app.nodes.iteritems() if v == 'slave']
                for _ in xrange(replicas - int(job['replicas'])):
                    node_id = slave_nodes.pop()
                    del app.nodes[node_id]
                    app.write_cluster(self.r)
                    node = schema.MonacoNode(node_id=node_id)
                    node.refresh(self.r)
                    node.apps.remove(app.app_id)
                    node.write(self.r)
                    logger.debug('Removed (%s)', node_id)
                cluster_changed = True

            if oldslavelb and not app.slavelb:
                # we're no longer slave-LB'ing this DB
                logger.debug('Deleting Slave exposure')
                self.expose.delete_app_slave(app)
            elif not oldslavelb and app.slavelb:
                # we're now slave-LB'ing this DB
                logger.debug('Creating Slave exposure')
                self.expose.create_app_slave(app)
            elif oldslavelb and app.slavelb and cluster_changed:
                # we're slave-LB'ing and the cluster has been updated
                logger.debug('Updating Slave exposure')
                self.expose.update_app_slave(app)

            logger.info('Waiting for all nodes to update the DB settings...')
            try:
                if not app.wait_status(self.r, revision=app.revision):
                    logger.error('Failed to acheive consistent updated revision across cluster. bug keith or something...')
                else:
                    logger.info('Updated app %s', app.app_id)
                    return True
            except Exception, exc:
                logger.exception(exc)
            return False

    def api_delete(self, job, logger):
        '''
        Delete app.
        job = {
            'app_id': app_id
        }
        '''
        # load app
        app = schema.App(app_id=str(job['app_id']))
        app.refresh(self.r)
        # delete exposure endpoint(s)
        if app.slavelb == 'True':
            logger.debug('Removing db %s slave exposure', app.app_id)
            self.expose.delete_app_slave(app)
        logger.debug('Removing db %s exposure', app.app_id)
        self.expose.delete_app(app)
        for node_id, _ in app.nodes.iteritems():
            logger.debug('Removing db instance from node %s', node_id)
            node = schema.MonacoNode(node_id=node_id)
            node.refresh(self.r)
            if app.app_id in node.apps:
                node.apps.remove(app.app_id)
                node.write(self.r)
        # bye bye
        app.delete(self.r)
        self.monaco.delete_app(app.app_id, self.r)
        logger.debug('Removing master subscription for app failover events')
        self.update_subs()
        logger.info('Deleted db %s', app.app_id)
        return True

    def api_migrate(self, job, logger):
        '''
        Migrate app instance.
        job = {
            'app_id': app_id
            'node_id_from': node_id
            'node_id_to': node_id
        }
        If node_id_from is the master, and node_id_to is a slave, their roles are swapped
        -- The below cases result in node_id_from deleting app_id
        If node_id_from is the master, and node_id_to is not in the cluster, node_id_to assumes master.
        If node_id_from is a slave, and node_id_to is not in the cluster, node_id_to assumes slave.
        '''
        logger.debug('Starting migrate-db job')
        app = schema.App(app_id=str(job['app_id']))
        app.refresh(self.r)
        if not job['node_id_from'] in app.nodes:
            logger.warn('Migrate job received with origin node not in the apps cluster.')
            return False
        if job['node_id_from'] == job['node_id_to']:
            logger.warn('Migrate received same origin and destination nodes.')
            return False

        if job['node_id_to'] in app.nodes:
            # Swapping roles (master and slave)
            logger.debug('Swapping master(%s) and slave(%s) roles.\nSetting new master to slave-write...', job['node_id_from'], job['node_id_to'])
            if app.nodes[job['node_id_to']] == app.nodes[job['node_id_from']]:
                logger.warn('Swapping two slave instances has no effect.')
                return False
            if app.nodes[job['node_id_from']] != 'master':
                logger.warn('Must migrate master to slave, not slave to master.')
                return False
            app.nodes[job['node_id_to']] = 'slave-write'
            app.update_revision()
            app.write_hash(self.r) # for new revision
            app.write_cluster(self.r) # for role changes

            logger.debug('Creating exposure entry to new master (%s)...')
            self.expose.create_app_host(app, self.monaco.hostnames_by_node_id[job['node_id_to']])
            logger.debug('Clearing old exposure to master (%s)...', job['node_id_from'])
            self.expose.delete_app_host(app, self.monaco.hostnames_by_node_id[job['node_id_from']])
            logger.info('Waiting for all nodes to update the transition replication settings...')
            try:
                if not app.wait_status(self.r, revision=app.revision):
                    logger.error('Failed to acheive consistent updated revision across cluster mid-migration!')
            except Exception, exc:
                logger.exception(exc)

            logger.debug('Updating permanent roles (%s->master, %s->slave)...', job['node_id_to'], job['node_id_from'])
            app.nodes[job['node_id_to']] = 'master'
            app.nodes[job['node_id_from']] = 'slave'
            app.update_revision()
            app.write_hash(self.r) # for new revision
            app.write_cluster(self.r) # for role changes
            logger.info('Waiting for all nodes to finalize the new replication settings...')
            try:
                if not app.wait_status(self.r, revision=app.revision):
                    logger.error('Failed to acheive consistent updated revision across cluster post-migration!')
            except Exception, exc:
                logger.exception(exc)
            # cleanup any residual weirdness
            self.expose.update_app(app)

        elif app.nodes[job['node_id_from']] == 'slave':
            logger.debug('Adding new slave instance (%s) to cluster, and deleting old slave instance (%s)', job['node_id_to'], job['node_id_from'])
            app.nodes[job['node_id_to']] = 'slave'
            del app.nodes[job['node_id_from']]
            newnode = schema.MonacoNode(node_id=job['node_id_to'])
            newnode.refresh(self.r)
            newnode.apps.append(app.app_id)
            oldnode = schema.MonacoNode(node_id=job['node_id_from'])
            oldnode.refresh(self.r)
            oldnode.apps.remove(app.app_id)
            oldnode.write(self.r)
            app.update_revision()
            app.write_hash(self.r) # for new revision
            app.write_cluster(self.r) # for role changes
            newnode.write(self.r) # so the new node will create the app
            logger.info('Waiting for all nodes to finalize the new replication settings...')
            try:
                if not app.wait_status(self.r, revision=app.revision):
                    logger.error('Failed to acheive consistent updated revision across cluster post-migration!')
            except Exception, exc:
                logger.exception(exc)

        else: # migrating master
            logger.debug('Creating new instance (%s) as slave-write', job['node_id_to'])
            app.nodes[job['node_id_to']] = 'slave-write'
            app.update_revision()
            app.write_hash(self.r) # for new revision
            app.write_cluster(self.r) # for role changes
            newnode = schema.MonacoNode(node_id=job['node_id_to'])
            newnode.refresh(self.r)
            newnode.apps.append(app.app_id)
            newnode.write(self.r)

            logger.debug('Creating exposure to new master (%s)...', job['node_id_to'])
            self.expose.create_app_host(app, self.monaco.hostnames_by_node_id[job['node_id_to']])
            logger.debug('Clearing old exposure to master (%s)...', job['node_id_from'])
            self.expose.delete_app_host(app, self.monaco.hostnames_by_node_id[job['node_id_from']])

            logger.info('Waiting for all nodes to update the transition replication settings...')
            try:
                if not app.wait_status(self.r, revision=app.revision):
                    logger.error('Failed to acheive consistent updated revision across cluster mid-migration!')
            except Exception, exc:
                logger.exception(exc)

            logger.debug('Updating permanent roles (%s->master, %s->None)', job['node_id_to'], job['node_id_from'])
            del app.nodes[job['node_id_from']]
            app.nodes[job['node_id_to']] = 'master'
            app.update_revision()
            app.write_hash(self.r) # for new revision
            app.write_cluster(self.r) # for role changes
            oldnode = schema.MonacoNode(node_id=job['node_id_from'])
            oldnode.refresh(self.r)
            oldnode.apps.remove(app.app_id)
            oldnode.write(self.r)
            try:
                if not app.wait_status(self.r, revision=app.revision):
                    logger.error('Failed to acheive consistent updated revision across cluster post-migration!')
            except Exception, exc:
                logger.exception(exc)

            self.expose.update_app(app)
        if app.slavelb == 'True':
            self.expose.update_app_slave(app)
        logger.info('Migration completed successfully')
        return True

    def api_maintenance(self, job, logger):
        '''
        Toggles maintenance status for anode
        Migrates all masters off node when entering maintenance from 'UP' state, no reverse behavior
        job = {
            'node_id': node_id
            'enable': True
        }
        '''
        #FIXME!!!
        node = schema.MonacoNode(node_id=job['node_id'])
        node.refresh(self.r)
        if node.status == 'MAINTENANCE':
            # Revert to normal operations
            self.monaco.node_up(node, self.r)
            return True
        oldstatus = node.status
        self.monaco.node_maintenance(node, self.r)
        if oldstatus == 'DOWN':
            # If down, just mark as maintenance, shouldn't be anything on it
            return True
        info = node.app_info(self.r)
        success = True
        for app_id in info['masters']:
            app = schema.App(app_id=app_id)
            app.refresh(self.r)
            size = int(app.maxmemory) // (1024 * 1024) # size in MB
            migrate_job = {
                'app_id': app_id,
                'node_id_from': job['node_id'],
                'node_id_to': self.select_master_node(size, select_from=[k for k, v in app.nodes.iteritems() if v == 'slave']),
            }
            if not migrate_job['node_id_to']:
                # If no slaves, just pick anything
                migrate_job['node_id_to'] = self.select_master_node(size)
            try:
                if self.api_migrate(migrate_job, logger):
                    logger.info('Performed api_migrate for maintenance (%s)', repr(migrate_job))
                else:
                    logger.error('Failed to migrate master off node for maintenaince!')
                    success = False
            except Exception, exc:
                logger.exception(exc)
                success = False
        return success

    def api_new_proxy(self, job, logger):
        '''
        Creates a twemproxy
        job = {
            ** schema.MonacoTwem.HASH_KEYS
            'servers': [app_id, app_id, ...]
            'extservers': ['server1_host:server1_port', ...]
        }
        '''
        # create the app structure
        args = {}
        for k in schema.MonacoTwem.HASH_KEYS:
            if not k in job and k in schema.MonacoTwem.ESSENTIAL_KEYS:
                logger.error('Expected field not provided (%s)', k)
                return False
            elif k in job:
                args[k] = job[k]
        if job['name'] in self.monaco.twem_ids_by_name:
            logger.warn('Proxy already exists with given name (%s)', job['name'])
            return False
        args['twem_id'] = self.monaco.next_twem_id(self.r)
        newtwem = schema.MonacoTwem(**args)
        newtwem.servers = job['servers']
        newtwem.extservers = job['extservers']
        newtwem.nodes = self.monaco.node_ids # all nodes carry all proxies for now
        newtwem.write(self.r)
        # now add the twem_id to nodes' twem set to trigger creation
        for node_id in self.monaco.node_ids:
            logger.debug('Adding proxy to node %s', node_id)
            node = schema.MonacoNode(node_id=node_id)
            node.refresh(self.r)
            node.twems.append(newtwem.twem_id)
            node.write(self.r)
        # configure the loadbalancer endpoint
        logger.debug('Creating exposure for proxy...')
        if not self.expose.create_twem(newtwem):
            return False
        # now add the app to the top-level monaco struct to indicate creation
        self.monaco.new_twem(newtwem.twem_id, newtwem.name, self.r)
        logger.info('New twem %s (%s)', newtwem.name, newtwem.twem_id)
        return True

    def api_delete_proxy(self, job, logger):
        '''
        Deletes a twemproxy
        job = {
            'twem_id': twem_id,
        }
        '''
        oldtwem = schema.MonacoTwem(twem_id=str(job['twem_id']))
        oldtwem.refresh(self.r)
        logger.debug('Deleting exposure for proxy...')
        if not self.expose.delete_twem(oldtwem):
            return False

        for node_id in oldtwem.nodes:
            logger.debug('Deleting proxy from node %s', node_id)
            node = schema.MonacoNode(node_id=node_id)
            node.refresh(self.r)
            node.twems.remove(oldtwem.twem_id)
            node.write(self.r)

        self.monaco.delete_twem(oldtwem.twem_id, self.r)
        oldtwem.delete(self.r)
        logger.info('Deleted twem %s (%s)', oldtwem.name, oldtwem.twem_id)
        return True

    def api_update_proxy(self, job, logger):
        '''
        This version won't change the node set, just conf
        job = {
            'twem_id': twem_id
            ** schema.MonacoTwem.HASH_KEYS, or a subset
            'servers': [app_id, app_id, ...], or unset
            'extservers': ['serverhost:serverport', ...], or unset
        }
        '''
        logger.debug('Buckle up, this is gonna take forever...')
        # Update DB
        logger.debug('Creating TWEM config struct')
        twem = schema.MonacoTwem(twem_id=job['twem_id'])
        twem.refresh(self.r)
        for key in twem.HASH_KEYS:
            if key in job:
                setattr(twem, key, job[key])
        if 'servers' in job:
            twem.servers = job['servers']
        if 'extservers' in job:
            twem.extservers = job['extservers']
        twem.write(self.r)

        # Sequentially update proxy hosts
        for node_id in twem.nodes:
            node = schema.MonacoNode(node_id=node_id)
            node.refresh(self.r)
            logger.debug('Disabling instance (%s) exposure...', node_id)
            if not self.expose.delete_twem_host(twem, node.hostname):
                return False
            logger.debug('Updating instance (%s)', node_id)
            node.trigger_twem_listener(self.r)
            logger.debug('Re-enabling instance (%s) exposure...', node_id)
            if not self.expose.create_twem_host(twem, node.hostname):
                return False
        logger.info('Updated twem %s (%s)', twem.name, twem.twem_id)
        return True

    def api_ping(self, job, logger):
        '''
        Logs/status the PONG:<timestamp>
        '''
        logger.info('PONG:%s', time.time())
        return True

##### helper methods #####
    def _nodes_on_rack(self, rack):
        '''
        Returns a list of node_ids on this rack
        '''
        node_ids = []
        for node_id in self.monaco.node_ids:
            node = schema.MonacoNode(node_id=node_id)
            node.refresh(self.r)
            if node.rack == rack:
                node_ids.append(node.node_id)
        self.logger.debug('_nodes_on_rack(%s): %s', rack, node_ids)
        return node_ids

    def _selectable_nodes(self, requested_size, exclude=(), overalloc=False, rack=None):
        '''
        Returns set of usable nodes
        requested_size: size in MB
        exclude: set of node_ids
        overalloc: True to use up to 95% of node RAM
        rack: if specified, only select nodes on given rack
        '''
        selectable_nodes = [n for n in self.monaco.node_ids if not n in exclude]
        selectable_nodes = [schema.MonacoNode(node_id=n) for n in selectable_nodes]
        for node in selectable_nodes:
            node.refresh(self.r)
        # Online
        selectable_nodes = [n for n in selectable_nodes if n.status == 'UP']
        # Rack selection
        if rack:
            selectable_nodes = [n for n in selectable_nodes if n.rack in rack]
        # Ram allocation
        selectable_nodes = [dict([('node', n)] + n.app_info(self.r).items()) for n in selectable_nodes]
        if overalloc:
            # Use up to 95% of the system max ram
            selectable_nodes = [n for n in selectable_nodes if (int(n['node'].total_memory) * 0.95) > (n['memory'] + requested_size)]
        else:
            # Use configured percent of system max ram
            selectable_nodes = [n for n in selectable_nodes if (int(n['node'].total_memory) * self.memory_utilization) > (n['memory'] + requested_size)]
        selectable_nodes.sort(cmp=lambda x, y: cmp(int(x['memory']), int(y['memory'])))

        self.logger.debug('_selectable_nodes(%s, exclude=%s, overalloc=%s, rack=%s): %s', requested_size, exclude, overalloc, rack, selectable_nodes)
        return selectable_nodes

    def select_master_node(self, requested_size, select_from=(), exclude=(), overalloc=False):
        '''
        Selects ideal node for a new master shard.
        requested_size: int size in MB
        select_from: list of node_id -- can be used to pass in existing slave shards if looking to promote
        exclude: list of node_id -- can be used to look for unique nodes not currently in cluster
        overalloc: default False, set to True to exceed configured memory limits
        '''
        if select_from and exclude:
            raise RuntimeError('Optionally specify selection set or exclusion set, not both')

        if select_from:
            nodes = [schema.MonacoNode(node_id=n) for n in select_from]
            for node in nodes:
                node.refresh(self.r)
            nodes = [dict([('node', n)] + n.app_info(self.r).items()) for n in nodes]
            nodes.sort(cmp=lambda x, y: cmp(int(x['memory']), int(y['memory'])))
        else:
            nodes = self._selectable_nodes(requested_size, exclude=exclude, overalloc=overalloc)
        nodes.sort(cmp=lambda x, y: cmp(len(x['masters']), len(y['masters'])))
        try:
            self.logger.debug('select_master_node(%s, select_from=%s, exclude=%s, overalloc=%s): %s', requested_size, select_from, exclude, overalloc, nodes[0]['node'].node_id)
            return nodes[0]['node'].node_id
        except IndexError:
            self.logger.debug('select_master_node(%s, select_from=%s, exclude=%s, overalloc=%s): None', requested_size, select_from, exclude, overalloc)
            return None

    def select_node(self, requested_size, exclude=(), overalloc=False):
        '''
        returns node_id of node that should receive next db of size
        requested_size: int size in MB
        exclude: list of node_id
        overalloc: default False, set to True to exceed configured memory limits
        '''
        nodes = self._selectable_nodes(requested_size, exclude=exclude, overalloc=overalloc)
        try:
            self.logger.debug('select_node(%s, exclude=%s, overalloc=%s): %s', requested_size, exclude, overalloc, nodes[0]['node'].node_id)
            return nodes[0]['node'].node_id
        except IndexError:
            self.logger.debug('select_node(%s, exclude=%s, overalloc=%s): None', requested_size, exclude, overalloc)
            return None

    def select_cluster(self, requested_size, replicas):
        '''
        Returns a new cluster allocation, or None if not possible
        requested_size: int size in MB
        replicas: total number of nodes
        '''
        nodes = {}
        exclude = []
        for idx in xrange(replicas):
            if idx == 0:
                # Select master based on master instance count
                node_id = self.select_master_node(requested_size)
                if not node_id:
                    self.logger.error('select_cluster(%s,%s): Could not select master', requested_size, replicas)
                    return None
                nodes[node_id] = 'master'
            else:
                # Select a node not on a currently deployed rack
                node_id = self.select_node(requested_size, exclude=exclude)
                if not node_id:
                    # If there's no available node excluding racks, allow rack re-use
                    self.logger.warn('select_cluster(%s,%s): Couldnt find rack-isolated slave host (cluster: %s)', requested_size, replicas, nodes)
                    node_id = self.select_node(requested_size, exclude=nodes.keys())
                if not node_id:
                    self.logger.error('select_cluster(%s,%s): Couldnt find slave host (cluster: %s)', requested_size, replicas, nodes)
                    return None
                nodes[node_id] = 'slave'
            # Exclude this racks nodes from being slave instances
            node = schema.MonacoNode(node_id=node_id)
            node.refresh(self.r)
            exclude += self._nodes_on_rack(node.rack)

        self.logger.debug('select_cluster(%s,%s): %s', requested_size, replicas, nodes)
        return nodes

    def offload_node(self, node_id):
        '''
        Offloads all user dbs on the node
        First attempts to promote a slave for all masters on the box,
        Then attempts to create slaves for all replica shards lost (slave or master)
        '''
        self.monaco.refresh(self.r)
        node = schema.MonacoNode(node_id=node_id)
        node.refresh(self.r)
        # get a list of user dbs that use this node for master/slave
        instance_masters = []
        instance_slaves = []
        for app_id in node.apps:
            app = schema.App(app_id=app_id)
            app.refresh(self.r)
            try:
                # last chance to save yourself..
                appcli = redis.StrictRedis(host=self.monaco.hostnames_by_node_id[node_id], port=app.port, socket_connect_timeout=1.0, socket_timeout=0.5)
                info = appcli.info()
                if info['role'] == app.nodes[node_id]:
                    if info['role'] == 'master':
                        continue
                    if info['role'] == 'slave' and info['master_host'] == node.FQDN:
                        continue
            except Exception:
                pass
            if app.master == node_id:
                instance_masters.append(app)
                toaddrs = [
                    app.operator,
                    config['admin_email'],
                ]
                sendmail(toaddrs,
                         'monaco@monaco.monaco',
                         'NOTICE: Unscheduled failover of DB Master (%s)' % app.app_id,
                         '''
    There were connectivity issues with the host your DB master shard was allocated to (%s). As a result, Monaco is promoting the most up-to-date slave replica, or spawning a new master instance if your DB was configured to run without replication. On completion of the transition, the loadbalanced VIP will be updated to reflect the new location of your master. Your client code may throw exceptions during the transition period but if you have properly implemented reconnect logic, the problem should resolve itself.
The app in reference: %s
Thanks for your understanding''' % (node.hostname, app.app_id),
                         config['smtp-server']
                )
            else:
                instance_slaves.append(app)
        # promote slaves for all downed masters
        for app in instance_masters:
            node.apps.remove(app.app_id)
            node.write(self.r)
            size = int(app.maxmemory) // (1024 * 1024) # size in MB
            if int(app.replicas) > 1:
                slave_set = [k for k, v in app.nodes.iteritems() if v == 'slave']
                try:
                    newmaster = app.best_slave(self.r)
                except RuntimeError:
                    # Just force one if we can't smartly select
                    self.logger.warn('Exception selecting best-slave from app %s', app.app_id)
                    newmaster = self.select_master_node(size, select_from=slave_set, overalloc=True)
                if not newmaster:
                    self.logger.error('No remaining slaves for app %s, DATA LOSS.', app.app_id)
                    newmaster = self.select_master_node(size, overalloc=True)
                    if not newmaster:
                        self.logger.error('No remaining host capable of carrying app %s! DOWN TIME', app.app_id)
                        continue
                masternode = schema.MonacoNode(node_id=newmaster)
                masternode.refresh(self.r)
                masternode.apps.append(app.app_id)
                del app.nodes[node_id]
                app.nodes[newmaster] = 'master'
                # These writes here to get slave promoted asap
                app.write(self.r)
                masternode.write(self.r)
                self.expose.update_app(app)
                self.logger.info('Promoted slave node (%s) to master for app %s', newmaster, app.app_id)
                # Settle the replica situation after settling all masters
                instance_slaves.append(app)
            else:
                newnode_id = self.select_master_node(size, exclude=app.nodes.keys(), overalloc=True)
                if not newnode_id:
                    self.logger.error('Failed to select a new master node for app %s! DOWN TIME', app.app_id)
                    continue
                del app.nodes[node_id]
                app.nodes[newnode_id] = 'master'
                newnode = schema.MonacoNode(node_id=newnode_id)
                newnode.refresh(self.r)
                newnode.apps.append(app.app_id)
                app.write(self.r)
                newnode.write(self.r)
                self.logger.warn('No replicas for app %s- new node (%s) will have complete data loss', app.app_id, newnode_id)
                self.expose.update_app(app)
                self.logger.info('Created master node (%s) for app %s', newnode_id, app.app_id)
        for app in instance_slaves:
            if app.app_id in node.apps:
                # might be a master failover that is getting a new slave node
                node.apps.remove(app.app_id)
                node.write(self.r)
            # select new node as slave
            size = int(app.maxmemory) // (1024 * 1024) # size in MB
            newnode_id = self.select_node(size, app.nodes.keys())
            if not newnode_id:
                self.logger.error('Failed to select a new slave node for app %s', app.app_id)
                continue
            app.nodes[newnode_id] = 'slave'
            if node_id in app.nodes:
                # might be removed from master failover
                del app.nodes[node_id]
            newnode = schema.MonacoNode(node_id=newnode_id)
            newnode.refresh(self.r)
            newnode.apps.append(app.app_id)
            app.write(self.r)
            newnode.write(self.r)
            self.logger.info('Added new slave node (%s) to app %s', newnode_id, app.app_id)
