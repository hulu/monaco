# Administrating a Monaco Cluster
Monaco is fairly hands-off, but should you need to move dbs, or perform maintenance on a host, the admin section of the web UI is your friend.

## Health & Status
In the admin section, you'll find the output of a master responsiveness test (displayed as the system latency), and the output of running a suite of system tests. Normally, the latency should be around .1s, but a slow network, or a busy API may cause fluctuations. Similarly, if the system tests run mid-API call, there may temporarily be a report of bad state. Any of these situations would be temporary, and could be verified by monitoring the master's log

## Cluster Info
In order to quickly discern which role a node has, the 'Nodes' tab of the admin page shows the Monaco Role, syncronization (so you can verify there are no splits in the cluster), as well as memory allocation.If you need to move databases because of unexpected growth, or just routine upgrades, you can click the node id, and you will see the details of that node. You could change the host names, rack identification, and from the allocation panel, move the databases hosted on that node.
All master instances can be migrated to a slave in their cluster (or any new node in a single node cluster), whereas slave instances can be migrated to any other node. This helps make the transitions more seamless with less data transfer being required to quickly restore the status of the user DB. (Depending on the robustness of your access methods, this can be seamless entirely).
