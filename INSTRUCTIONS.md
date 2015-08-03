# Instructions for Monaco
There's not too much that needs to be set up before starting monaco, but this file will help get you going

## Clustering
You'll need to establish an initial "Raft" cluster (of 3 nodes at least). This is the (sub)set of nodes which will participate in leadership elections. As you grow your cluster to fit capacity, you will likely not need any more members in the Raft algorithm if you have selected well diversified nodes.<br>
To see an example configuration of a Raft node see [monaco_elector.conf](https://github.com/hulu/monaco/config/monaco_elector.conf)
You'll want to edit the 'cluster' field in the 'raft' section of the config. It is a yaml list of server IP:Port pairs in list form.

## Initializing
It's not quite ready yet- because Monaco relies on it's underlying redis-db for persistence of state, it will need to be initialized for usage. From any node of the cluster:

```
	$ python /usr/monaco/init.py
	$ sudo start monaco-web 
```
Now with the webserver running, you can go to the admin panel and use 'add node' to add the "Raft" nodes to the cluster. Now Monaco can be started:

```
	$ sudo start monaco
```
### Joining the cluster
Again, since Monaco relies on the underlying redis-db, when a new node is started for the first time, the db must be initialized (essentially just slave'd to any existing db):

```
	$ python /usr/monaco/init.py master_host master_port
	$ sudo start monaco
```
**Note**: This must be done after adding nodes through the web UI.

## Growing the cluster
Even as your cluster is getting larger to host more Redis DBs, you probably won't need more nodes competing to be Monaco master- a set of systems, which a majority of members are expected to be available should be sufficient. To add nodes in a non-master mode, repeat the above steps, but refer to [monaco_storage.conf](https://github.com/hulu/monaco/config/monaco_storage.conf)

## Administrating
Monaco is fairly hands off, but you may find you want to rebalance the cluster, or perform maintanence. These methods are described in detail in [Administration](https://github.com/hulu/monaco/blob/master/ADMINISTRATION.md).
