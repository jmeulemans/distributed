import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.*;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Server {
    static Queue<NodeClient> computeNodes;
    private static ServerHandler handler;
    private static FileOutputStream fw;
    protected static TMultiplexedProcessor processor;

    private static int computeNodesPerJob = 2;
    private static int chunkSize = 1000000; // 1 megabyte
    public volatile static int id = 0;
    public static int nodeID = 0;
    public static int mergeSize;
    public static volatile int redoneTasks = 0;
    public static volatile int totalTasks = 0;
    public static volatile int haltedTasks  = 0;
    private static float failProb;


    public static void main(String[] args) {
        try {
            org.apache.log4j.BasicConfigurator.configure();
            String ip = InetAddress.getLocalHost().getHostAddress();
            int port = 9090;

            mergeSize = Integer.parseInt(args[0]);
            failProb = Float.parseFloat(args[1]);
            fw = new FileOutputStream("server_list.txt");
            fw.write((ip + ":" + port + "\n").getBytes());
            fw.flush();
            fw.getFD().sync();

            computeNodes = new PriorityBlockingQueue<>();
            handler = new ServerHandler();
            processor = new TMultiplexedProcessor();
            processor.registerProcessor("ClientToServerService", new ClientToServerService.Processor(handler));
            processor.registerProcessor("NodeToServerService", new NodeToServerService.Processor(handler));
            TServerTransport serverTransport = new TServerSocket(port);
            TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport).processor(processor);
            TServer server = new TThreadPoolServer(serverArgs);
            server.serve();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    private static List<NodeClient> getJobNodes() {
        List<NodeClient> clientsCopy = new ArrayList<>(computeNodes);
        Collections.shuffle(clientsCopy);
        return clientsCopy.subList(0, Math.min(computeNodesPerJob, clientsCopy.size()));
    }

    public abstract static class Task implements Runnable {
        public final Lock lock;
        public List<String> results;
        public Queue<Task> tasks;
        public volatile String result;
        public volatile boolean completed = false;
        public List<NodeClient> taskNodes;


        public Task(Lock lock, List<String> results, Queue<Task> tasks) {
            this.lock = lock;
            this.results = results;
            this.tasks = tasks;
        }

        abstract String operation(NodeClient taskNode, int id);

        @Override
        public void run() {
            //For a succcessful run, the number of redone tasks will be incremented and decremented
            //Otherwise, it is only ever incremented, indicating that a task was started but did not complete
            Server.redoneTasks++;
            //Get available compute nodes, insure that they are all still alive before starting task
            taskNodes = getJobNodes();
            List<NodeClient> failedNodes = new ArrayList<>();
            for (NodeClient taskNode : taskNodes) {
                if (!taskNode.ping()) {
                    failedNodes.add(taskNode);
                    if (computeNodes.contains(taskNode)) {

                        computeNodes.remove(taskNode);
                    }
                }
            }
            //Cleanup any discovered dead compute nodes
            for (NodeClient failedNode : failedNodes) {
                taskNodes.removeIf(nodeClient -> nodeClient.equals(failedNode));
            }
            if(taskNodes.isEmpty())
            {
                //computation cannot be performed
                return;
            }
            //Set up executor, gather unique ids for each redundant task
            ExecutorService executor = Executors.newFixedThreadPool(taskNodes.size());

            List<Integer> ids = new ArrayList<>();
            for (int i = 0; i < taskNodes.size(); i++) {
                ids.add(++Server.id);
            }
            //Deploy each redundant task
            for (int i = 0; i < taskNodes.size(); i++) {
                Server.totalTasks++;
                executor.execute(new SubTask(taskNodes.get(i), this, ids.get(i), ids));
            }
            executor.shutdown();
            Thread current = Thread.currentThread();
            //result is volatile, continue to check at 1 ms intervals until some redundant task has completed
            //once a redundant task has completed, result will no longer be null
            while (result == null && !executor.isTerminated()) {
                try {
                    current.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                }
            }
            //shutdown any remaining threads that are still executing
            if (!executor.isTerminated()) {
                executor.shutdownNow();
            }
            //Task is complete if the result isn't null, remove this task
            if (result != null) {


                synchronized (lock) {
                    results.add(result);
                }
                redoneTasks--;
                tasks.remove(this);
            }
        }

    }

    public static class SubTask implements Runnable {
        public NodeClient taskNode;
        public Task task;
        public final int id;
        public List<Integer> ids;


        public SubTask(NodeClient taskNode, Task task, int id, List<Integer> ids) {
            this.taskNode = taskNode;
            this.task = task;
            this.id = id;
            this.ids = ids;
        }

        @Override
        public void run() {
            //perform the requisite sort or merge, only set result if this is the first thread to complete
            //and only set result if its not null
            //kill any other redundant tasks if the above conditions hold
            String result = task.operation(taskNode, id);
            if (result != null && !task.completed) {
                task.completed = true;
                task.result = result;
                for (int i = 0; i < task.taskNodes.size(); i++) {
                    if (ids.get(i) != id) {
                        if (task.taskNodes.get(i).kill(ids.get(i))) {
                            haltedTasks++;
                        }
                    }
                }
            }
        }

    }

    public static class SortTask extends Task {
        public String fileName;
        public int offset;
        public int length;

        public SortTask(Lock lock, List<String> results, Queue<Task> tasks, String fileName, int offset, int length) {
            super(lock, results, tasks);
            this.fileName = fileName;
            this.offset = offset;
            this.length = length;
        }

        @Override
        String operation(NodeClient taskNode, int id) {
            try {
                return taskNode.sort(fileName, offset, length, id);
            } catch (TException e) {
                e.printStackTrace();
                return null;
            }
        }
    }


    public static class MergeTask extends Task {
        public List<String> fileNames;

        public MergeTask(Lock lock, List<String> results, Queue<Task> tasks, List<String> fileNames) {
            super(lock, results, tasks);
            this.fileNames = fileNames;

        }

        @Override
        public String operation(NodeClient taskNode, int id) {
            try {
                String result = taskNode.merge(fileNames, id);
                return result;
            } catch (TException e) {
                e.printStackTrace();
                return null;
            }
        }

    }

    static class ServerHandler implements NodeToServerService.Iface, ClientToServerService.Iface {

        @Override
        public String registerNode(String ip, int port) throws TException {
            System.out.println("Registering compute node - " + ip + ":" + port);
            NodeClient newClient = new NodeClient(ip, port);
            computeNodes.add(newClient);
            //add new file server to list of servers
            synchronized (fw) {
                try {
                    fw.write((newClient.toString() + "\n").getBytes());
                    fw.flush();
                    fw.getFD().sync();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
            String result = nodeID + ":" + failProb;
            nodeID++;
            return result;
        }

        @Override
        public String sort(String filename) throws TException {
            System.out.println("Sorting " + filename);
            for(NodeClient taskNode : computeNodes)
            {
                taskNode.notifyOfJob();
            }
            try {
                List<String> sortedFiles = new ArrayList<>();
                //lock to protect sorted files
                final Lock listLock = new ReentrantLock();
                Queue<Task> tasks = new ConcurrentLinkedQueue<>();
                setupSortTasks(filename, sortedFiles, listLock, tasks);
                //run all sort tasks, results are added to sorted files
                if (!runTasks(tasks)) {
                    return "Could not sort " + filename;
                }
                while (true) {
                    //run merges until only 1 file remains, indicating completion
                    int size = sortedFiles.size();
                    setupMergeTasks(sortedFiles, listLock, tasks, size);
                    if (runTasks(tasks)) {
                        for (int i = size - 1; i >= 0; i--) {
                            sortedFiles.remove(i);
                        }
                        if (sortedFiles.size() == 1) {
                            String finalFileName = sortedFiles.get(0);
                            int finalID = Integer.parseInt(finalFileName.substring(0, finalFileName.indexOf('.')));
                            String newFileName = filename + "_OUTPUT";
                            File oldFile = new File(finalFileName);
                            File newFile = new File(newFileName);
                            boolean success = oldFile.renameTo(newFile);
                            if (!success) {
                                System.out.println("Could not move file.");
                            }

                            for (int i = 0; i <= Server.id; i++) {
                                try {
                                        Files.deleteIfExists(Paths.get(i + ".sort"));
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                            return newFileName;
                        }
                        if (sortedFiles.size() == 0) {
                            return "Error: No files to merge";
                        }
                    } else {
                        return "Could not sort " + filename;
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
                return "IOException occurred";
            } catch (InterruptedException e) {
                return e.getMessage();
            }
        }

        @Override
        public String getInfoFromServer() throws TException {
            String result = "";
            if(computeNodes.isEmpty())
            {
                result += "No active compute nodes\n";
            }
            for(NodeClient client : computeNodes)
            {
                result += client + " - ";
                String nodeResult = client.getInfoFromNode();
                if(nodeResult != null)
                {
                    result += nodeResult +"\n";
                }
                else
                {
                    result += "unable to retrieve info \n";
                }
            }
            result += "Server - "+totalTasks+ " tasks deployed, "+haltedTasks+" redundant tasks stopped, "+redoneTasks+" total faults \n";
            return result;
        }

        @Override
        public boolean setParameter(KeyValue parameter) throws TException {
            String name = parameter.getName();
            String value = parameter.getValue();
            if("chunksize".equals(name))
            {
                int chunksize = Integer.parseInt(value);
                if(chunksize<=0)
                {
                    return false;
                }
                Server.chunkSize = chunksize;
                return true;
            }
            else if("nodespertask".equals(name))
            {
                int nodesperjob = Integer.parseInt(value);
                if(nodesperjob < 1)
                {
                    return false;
                }
                Server.computeNodesPerJob = nodesperjob;
                return true;
            }
            else if("taskspermerge".equals(name))
            {
                int mergesize = Integer.parseInt(value);
                if(mergesize<2)
                {
                    return false;
                }
                Server.mergeSize = mergesize;
                return true;
            }
            else if("failprob".equals(name))
            {
                boolean noError = true;
                for(NodeClient client : computeNodes) {
                    if(!client.setParameter(parameter))
                    {
                        noError = false;
                    }
                }
                return noError;

            }
            return false;

        }

        private void setupMergeTasks(List<String> sortedFiles, Lock listLock, Queue<Task> tasks, int size) {
            for (int i = 0; i < size; i += mergeSize) {
                List<String> mergeFiles = new ArrayList<>(sortedFiles.subList(i, Math.min(i + mergeSize, size)));
                tasks.add(new MergeTask(listLock, sortedFiles, tasks, mergeFiles));

            }
        }

        private void setupSortTasks(String filename, List<String> sortedFiles, Lock listLock, Queue<Task> tasks) throws IOException {
            FileInputStream fileStream;
            File f = new File(filename);
            int fileSize = (int) f.length();
            fileStream = new FileInputStream(filename);
            int numSortsNeeded = (fileSize / chunkSize) + 1;
            int nextPosition = 0;
            int offset = 0;
            int taskNumber;
            for (taskNumber = 0; taskNumber < numSortsNeeded - 1; taskNumber++) {
                fileStream.skip(chunkSize - 1);
                nextPosition += chunkSize - 1;
                byte[] nextChar = new byte[1];
                fileStream.read(nextChar, 0, 1);
                nextPosition++;
                while ((char) nextChar[0] != ' ') {
                    fileStream.read(nextChar, 0, 1);
                    nextPosition++;
                }
                int length = nextPosition - offset;
                tasks.add(new SortTask(listLock, sortedFiles, tasks, filename, offset, length));
                offset = nextPosition;
            }
            //handle the last portion of the file
            //TODO: if final result not right, check here for off by one error
            tasks.add(new SortTask(listLock, sortedFiles, tasks, filename, offset, fileSize - offset));
        }

        private boolean runTasks(Queue<Task> tasks) throws InterruptedException {
            ExecutorService executorService;
            // continually execute every task until they all complete, or a timeout has occurred
            while (!tasks.isEmpty()) {
                if (computeNodes.isEmpty()) {
                    System.out.println("Failure: no compute nodes available.");
                    return false;
                }
                executorService = Executors.newFixedThreadPool(5);
                //TODO redo the nodeclients that the task has
                for (Task task : tasks) {
                    executorService.execute(task);
                }
                executorService.shutdown();
                if (!executorService.awaitTermination(3, TimeUnit.MINUTES)) {
                    executorService.shutdownNow();
                    return false;
                }
            }
            return true;
        }
    }

    static class NodeClient implements ServerToNodeService.Iface, Comparable {
        private int port;
        private String ip;

        public NodeClient(String ip, int port) {
            try {
                this.ip = ip;
                this.port = port;

            } catch (Exception x) {
                x.printStackTrace();
            }
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof NodeClient) {
                NodeClient other = (NodeClient) o;
                return this.port == other.port && this.ip.equals(other.ip);
            }
            return false;
        }

        public String toString() {
            return ip + ":" + port;
        }

        @Override
        public String sort(String filename, int offset, int len, int id) throws TException {
            TSocket transport = new TSocket(ip, port);
            TProtocol protocol = new TBinaryProtocol(transport);
            TMultiplexedProtocol coordProtocol = new TMultiplexedProtocol(protocol, "ServerToNodeService");
            ServerToNodeService.Client client = new ServerToNodeService.Client(coordProtocol);
            try {
                transport.open();
                String result = client.sort(filename, offset, len, id);
                transport.close();
                return result;
            } catch (TException e) {
                return null;
            }

        }

        @Override
        public String merge(List<String> filenames, int id) throws TException {
            TSocket transport = new TSocket(ip, port);
            TProtocol protocol = new TBinaryProtocol(transport);
            TMultiplexedProtocol coordProtocol = new TMultiplexedProtocol(protocol, "ServerToNodeService");
            ServerToNodeService.Client client = new ServerToNodeService.Client(coordProtocol);
            try {
                transport.open();
                String result = client.merge(filenames, id);
                transport.close();
                return result;
            } catch (TException e) {
                System.out.println("Compute node died");
                return null;
            }
        }

        @Override
        public boolean kill(int id) {
            TSocket transport = new TSocket(ip, port);
            TProtocol protocol = new TBinaryProtocol(transport);
            TMultiplexedProtocol coordProtocol = new TMultiplexedProtocol(protocol, "ServerToNodeService");
            ServerToNodeService.Client client = new ServerToNodeService.Client(coordProtocol);
            try {
                transport.open();
                boolean result = client.kill(id);
                transport.close();
                return result;
            } catch (TException e) {
                return false;
            }
        }

        @Override
        public void notifyOfJob() {
            TSocket transport = new TSocket(ip, port);
            TProtocol protocol = new TBinaryProtocol(transport);
            TMultiplexedProtocol coordProtocol = new TMultiplexedProtocol(protocol, "ServerToNodeService");
            ServerToNodeService.Client client = new ServerToNodeService.Client(coordProtocol);
            try {
                transport.open();
                client.notifyOfJob();
                transport.close();
            } catch (TException e) {
                return;
            }
        }

        @Override
        public boolean setParameter(KeyValue parameter) throws TException {
            TSocket transport = new TSocket(ip, port);
            TProtocol protocol = new TBinaryProtocol(transport);
            TMultiplexedProtocol coordProtocol = new TMultiplexedProtocol(protocol, "ServerToNodeService");
            ServerToNodeService.Client client = new ServerToNodeService.Client(coordProtocol);
            boolean result = false;
            try {
                transport.open();
                result = client.setParameter(parameter);
                transport.close();
            } catch (TException e) {
                System.out.println("Unable to set parameter "+ parameter.getName());
            }
            return result;
        }

        @Override
        public String getInfoFromNode() throws TException {
            TSocket transport = new TSocket(ip, port);
            TProtocol protocol = new TBinaryProtocol(transport);
            TMultiplexedProtocol coordProtocol = new TMultiplexedProtocol(protocol, "ServerToNodeService");
            ServerToNodeService.Client client = new ServerToNodeService.Client(coordProtocol);
            try {
                transport.open();
                String result = client.getInfoFromNode();
                transport.close();
                return result;
            } catch (TException e) {
            }
            return null;
        }

        @Override
        public boolean ping() {
            try {
                TSocket transport = new TSocket(ip, port);
                TProtocol protocol = new TBinaryProtocol(transport);
                TMultiplexedProtocol coordProtocol = new TMultiplexedProtocol(protocol, "ServerToNodeService");
                ServerToNodeService.Client client = new ServerToNodeService.Client(coordProtocol);
                transport.open();
                boolean result = client.ping();
                transport.close();
                return result;
            } catch (TException e) {
                return false;
            }
        }

        @Override
        public int compareTo(Object o) {
            return ThreadLocalRandom.current().nextInt(-1, 2);
        }
    }
}
