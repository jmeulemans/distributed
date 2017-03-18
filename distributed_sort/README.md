**Distributed Sort**
====================
**Information**
A distributed map-reduce sorting implementation built on the Thrift framework. This user can select redundancy of computations, and an analysis is provided for the trade off of performance and reliability as this redundancy is varied.
**Authors**
Jeremy Meulemans and Connor McMahon

**User Guide:**

Compilation: 

Start with clean copy of the `distributed_sort/` directory. From this directory,  run `./compile.sh`.

Server: 

To start the server, run `./server.sh <files_per_merge> <universal_fail_prob>` where `files_per_merge` is the number of files processed during one merge and `universal_fail_prob` is the failure probability assigned to each compute node.

Compute Node: 

To add a compute node, on the same machine as the Server, or a remote machine, run `./computenode.sh <port>`. This will register the compute node with the server, and start the compute node on the provided port. 

User Interface:

To interact with the system, set up a server and the requisite number of compute nodes. A client can then be started with the command `./client.sh server_list.txt`, where `server_list.txt` is a list of all the compute nodes in the system maintained by the running server. The file `server_list.txt` should be present in the directory if running on cselabs.

The commands to interact with the system are:

1. `sort,<filename>` - sort a filename specified by the user, returning the filename of the sorted file, or an error, as reported by the server. This file should be located in the pa3/ directory.

2. `info`, print information about each compute node, including number of tasks received and average time per task. Information is also printed about the server, including the number of faults that have occurred, and the number of tasks sent, and the number of redundant tasks killed.

3. `set,chunksize,<value>` - sets the chunksize to be processed

4. `set,failprob,<value>` - sets the fail probability of all of the nodes

5. `set,nodesperjob,<value>` - sets the number of nodes to run for each task

6. `set,taskspermerge,<value>` - sets the number of files to be merged per merge round

7. `quit` - quit the client program

Testing:

Create a system with 1 server and at least 4 compute nodes. Sort a file and note the name of the resultant sorted file `<sorted file>`. Then run `python verify.py <sorted file> <verification file>`, where `<verification file>` is a known sorted version of the input file independent of the output of the system. For example, `python verify.py 2000000_OUTPUT 2000000_sorted`. This script will print the count of numbers in each file and then `True` or `False` as to whether the files have the same numbers in the same order. This `True` or `False` indicates the success of the sort. 

**Design:**
	 
**Client.java:**

Summary: Implementation of Client construct

Important attributes:

* `ServerClient` - how the client interacts with the server

Important Functions:

Main body

* `parseServers` - parses the information from server_list.txt to create a ServerClient

ServerClient

* `sort` - client implementation of sort from ClientToServerService interface

* `getInfoFromServer` - client implementation of getInfoFromServer from ClientToServerService interface

* `setParameter` - client implementation of setParameter from ClientToServerService interface

**Server.java:**

Summary:  Implementation of the Server construct

Important attributes:

* `computeNodes` - a thread safe queue keeping track of the compute nodes available to the server

* `mergeSize` - how many files should be merged at once

* `failProb` - a global failure probability for all compute nodes

* `computeNodesPerTask` - how many compute nodes should be assigned to a task (if > 1, the rest of the nodes are redundant to provide robustness against node failure)

* `id` - a global variable used to assign a unique id to each task.

Important Functions:

Main body

* `getJobNodes` - returns a random list of jobs the size of computeNodesPerTask for each Task Runnable

**ServerHandler**

* `sort` - the server facing implementation of Sort in the ClientToServerService interface, controls the dispatch of all sort and merge tasks.

* `registerNode` - the server facing implementation of registerNode in the NodeToServerService interface, loading the node information into computeNodes

* `setUpMergeTasks` - private method that sets up merge tasks

* `setUpSortTasks` - private method that sets up sort tasks

* `getInfoFromNode` - queries computeNode information and returns it to the client

* `setParameter` - sets the parameter values, forwarding them to the compute nodes if neccessary

* `runTasks` - a private method to actually run what tasks are generated in setUpMergeTasks or setUpSortTasks, restarting tasks as necessary until all successfully complete, or all compute nodes in the system have failed. 

**NodeClient**

* `sort` - client facing implementation of sort in ServerToNodeService interface

* `merge` - client facing implementation of merge in ServerToNodeService interface

* `kill~ - client facing implementation of kill in ServerToNodeService interface

* `ping` - client facing implementation of ping in ServerToNodeService interface

* `notifyOfJob` - client facing implementation of notifyOfJob in ServerToNodeService interface

* `setParameter` - client facing implementation of setParameter in ServerToNodeService interface

* `getInfoFromNode` - client facing implementation of getInfoFromNode in ServerToNodeService interface

**ComputeNode.java:**

Summary: Our implementation of the compute node construct

Important attributes:

* `handler`- handles requests from server

* `runningProcess` - a map from ids to threads for redundant subtasks to be killed 

Important Functions:

Main body

* `registerRunnable` - private method to set up a compute node

ComputeNodeServerHandler

* `sort` - performs the sort of the chunk it receives and writes to a new file

* `merge` -  performs a merge of the files received and writes to a new file

* `notifyOfJob` - tests to see if the compute node will fail for this job, and if it will, sets a random time interval for it to fail at, so it can fail during either a sort or a merge

* `getInfoFromNode` - returns the node information to the server

ServerClient

* `registerNode` - sends ip and port to the server so the server knows how to contact the compute node.

**Performance:**

**System**

For all of the performance evaluations, a system with 4 compute nodes, 1 server, and 1 client was setup. This system was deployed across 6 different cselabs machines, to measure performance in the fully distributed case. This system was deployed using the scripts indicated in the USER section of this document. Note that all filesizes indicate the count of numbers in the file, whereas chunksize indicates the size of the chunks in bytes.

**Varying Chunk Size and File Size **

The system was first tested with varying chunk sizes and file sizes, to establish a baseline for later measurements. For each of the tests, the number of files per merge task was 4, the probability of node failure was 0, and 2 compute nodes were assigned per task. In this way, no nodes would fail, and the redundancy of the system was still utilized for each sort/merge task. After the completion of each job, the correctness of the sort was verified with the verify.py script. 

<table>
  <tr>
    <td>File size/Chunk size</td>
    <td>200000 #s </td>
    <td>1000000 #s</td>
    <td>2000000#s</td>
    <td>6000000#s</td>
    <td>10000000#s</td>
    <td>20000000#s</td>
  </tr>
  <tr>
    <td>200000 bytes</td>
    <td>1.595</td>
    <td>11.86</td>
    <td>13.679</td>
    <td>50.052</td>
    <td>82.441</td>
    <td>189.803</td>
  </tr>
  <tr>
    <td>500000 bytes</td>
    <td>1.939</td>
    <td>7.193</td>
    <td>16.405</td>
    <td>42.509</td>
    <td>83.571</td>
    <td>154.341</td>
  </tr>
  <tr>
    <td>1000000 bytes</td>
    <td>3.039</td>
    <td>9.03</td>
    <td>12.058</td>
    <td>47.459</td>
    <td>73.776</td>
    <td>171.751</td>
  </tr>
  <tr>
    <td>2000000 bytes</td>
    <td>1.581</td>
    <td>5.232</td>
    <td>15.814</td>
    <td>34.39</td>
    <td>76.467</td>
    <td>141.437</td>
  </tr>
</table>


With some variance, it does appear that a larger chunk size tends towards faster performance for the system. For chunk sizes that are greater than the file sizes (only for the 200000 number file), there is a huge increase in performance. In such cases, there is one sort task, and no merges performed, resulting in the observed performance increase. 

**Varying the Probability of Failure **

For each of the file sizes of the previous section, the chunk size resulting in the overall lowest sort time was selected. The system was otherwise the same as described previously, with 4 files per merge task and 2 compute nodes per job. For each entry in the results below, the first element of the tuple is the time to complete the sort in seconds, and the second element is the number of nodes that failed in the process of running the sort. A value of N/A indicates job could not be completed. For jobs that successfully completed, the correctness of the final output file was verified with the verify.py script. 

<table>
  <tr>
    <td></td>
    <td></td>
    <td>Failure Probability</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>Filesize</td>
    <td>Chunksize</td>
    <td>0.1</td>
    <td>0.2</td>
    <td>0.3</td>
    <td>0.5</td>
    <td>0.75</td>
    <td>0.9</td>
  </tr>
  <tr>
    <td>200,000</td>
    <td>2,000,000</td>
    <td>(6.997, 1)</td>
    <td>(2.425, 0)</td>
    <td>(11.87, 1)</td>
    <td>(2.441, 0)</td>
    <td>(14.756, 2)</td>
    <td>(29.582, 3)</td>
  </tr>
  <tr>
    <td>1000000</td>
    <td>2,000,000</td>
    <td>(6.625,0)</td>
    <td>(8.307,0)</td>
    <td>(20.045,2)</td>
    <td>(10.581,0)</td>
    <td>(N/A,4)</td>
    <td>(24.691,2)</td>
  </tr>
  <tr>
    <td>2000000</td>
    <td>1000000</td>
    <td>(17.596, 0)</td>
    <td>(13.036, 0)</td>
    <td>(19.187,0)</td>
    <td>(22.162, 3)</td>
    <td>(22.427, 1)</td>
    <td>(N/A, 4)</td>
  </tr>
  <tr>
    <td>6000000</td>
    <td>2000000</td>
    <td>(53.305,1)</td>
    <td>(39.33, 0)</td>
    <td>(39.43, 0)</td>
    <td>(66.936,3)</td>
    <td>(N/A,4)</td>
    <td>(N/A,4)</td>
  </tr>
  <tr>
    <td>10000000</td>
    <td>1000000</td>
    <td>(91.832,1)</td>
    <td>(77.883,1)</td>
    <td>(78.829, 1)</td>
    <td>(86.575, 1)</td>
    <td>(115.844, 3)</td>
    <td>(101.929, 3)</td>
  </tr>
  <tr>
    <td>20000000</td>
    <td>2000000</td>
    <td>(150.571, 0)</td>
    <td>(173.581, 0)</td>
    <td>(139.971,0)</td>
    <td>(297.793,3)</td>
    <td>(N/A,4)</td>
    <td>(N/A, 4)</td>
  </tr>
</table>


From the above data, it appears that the determining factor for sort performance was not just the probability of failure, but the number of nodes that failed. In general, the more nodes that failed, the slower the computation. As the probability of failure increased, the number of nodes failing trends upward as expected, which is what actually caused the increases in sort time. This can be evidenced by higher fail percentages having better performance when fewer nodes actually failed. This is due to the random nature of failures.

**Varying the nodes per task**

To determine the performance of the system for varying numbers of nodes per task, the system was assigned a probability of failure of 0 for each compute node. Then, the number of nodes per task was varied, from a single compute node performing each sort or merge task, to all four performing each sort and merge task. The results are below.

 

<table>
  <tr>
    <td></td>
    <td></td>
    <td>Nodes per task</td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>Filesize (#s)</td>
    <td>Chunksize</td>
    <td>1</td>
    <td>2</td>
    <td>3</td>
    <td>4</td>
  </tr>
  <tr>
    <td>200000</td>
    <td>1000000</td>
    <td>1.526</td>
    <td>1.982</td>
    <td>1.3917</td>
    <td>1.272</td>
  </tr>
  <tr>
    <td>1000000</td>
    <td>1000000</td>
    <td>7.704</td>
    <td>8.8427</td>
    <td>8.279</td>
    <td>8.972</td>
  </tr>
  <tr>
    <td>2000000</td>
    <td>1000000</td>
    <td>12.396</td>
    <td>15.342</td>
    <td>18.192</td>
    <td>19.176</td>
  </tr>
  <tr>
    <td>6000000</td>
    <td>1000000</td>
    <td>44.112</td>
    <td>46.322</td>
    <td>51.912</td>
    <td>58.511</td>
  </tr>
  <tr>
    <td>10000000</td>
    <td>1000000</td>
    <td>66.525</td>
    <td>72.735</td>
    <td>80.457</td>
    <td>94.369</td>
  </tr>
  <tr>
    <td>20000000</td>
    <td>1000000</td>
    <td>159.771</td>
    <td>174.741</td>
    <td>222.924</td>
    <td>273.328</td>
  </tr>
</table>


The results of varying the number of nodes per task are as expected. For the small 200000 number file, all assignments of nodes per task are roughly equal. This is because, in all cases, there is only 1 sort task that must be performed, as the chunksize is greater than the file size. Therefore, 1 or more nodes are performing the single task, and the times are very comparable. 

Outside of this file, generally systems with 1 node per task perform the fastest, as the fewest overall tasks must be performed. However, this system is also the most susceptible to failures. As the number of nodes per task increases, the performance is decreased, because each node performs more tasks. The reliability of the system goes up though, as more redundant computations are performed with increases in nodes per task, so that the chances of some node completing the task goes up. For a system of 4 nodes per task, each node performs all tasks, resulting in the worst performance. 

Generally speaking, performance comes at the cost of reliability. Increased system redundancy increases the reliability of the system in terms of successfully completing the sort, with added computation time. 

